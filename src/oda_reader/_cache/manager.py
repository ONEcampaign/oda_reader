"""File-based cache manager for bulk downloads.

Inspired by pydeflate's cache design, this module provides a robust caching system
for large, monolithic datasets (bulk CRS, Multisystem, AidData downloads).
"""

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from filelock import FileLock

from oda_reader._cache.config import get_bulk_cache_dir

logger = logging.getLogger("oda_reader")

# ISO format for datetime serialization
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


@dataclass(frozen=True)
class CacheEntry:
    """Immutable descriptor for a cacheable bulk dataset.

    Attributes:
        key: Unique identifier (e.g., "crs_full", "aiddata")
        filename: Storage filename (e.g., "crs_full.parquet", "aiddata.parquet")
        fetcher: Callable that downloads and writes data to a given path
        ttl_days: Time-to-live in days (default: 30)
        version: Optional version string for cache invalidation
    """

    key: str
    filename: str
    fetcher: Callable[[Path], None]
    ttl_days: int = 30
    version: Optional[str] = None


class CacheManager:
    """Manages cached bulk files with a JSON manifest.

    Provides:
    - Atomic writes (temp file + rename)
    - Cross-process locking (FileLock)
    - TTL-based expiration
    - Version-based invalidation
    - Observable cache state (list_records, stats)

    Storage layout:
        {base_dir}/
        ├── manifest.json       # Metadata tracking
        ├── .cache.lock         # FileLock for coordination
        └── *.parquet           # Cached datasets
    """

    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize cache manager.

        Args:
            base_dir: Directory for cache storage. If None, uses get_bulk_cache_dir().
        """
        self.base_dir = base_dir or get_bulk_cache_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.manifest_path = self.base_dir / "manifest.json"
        self.lock_path = self.base_dir / ".cache.lock"
        self._lock = FileLock(str(self.lock_path), timeout=1200)

    def ensure(self, entry: CacheEntry, refresh: bool = False) -> Path:
        """Ensure cached file exists, fetching if necessary.

        Args:
            entry: CacheEntry describing the dataset
            refresh: If True, bypass cache and fetch fresh data

        Returns:
            Path: Path to the cached file

        Example:
            >>> manager = bulk_cache_manager()
            >>> entry = CacheEntry(
            ...     key="crs_full",
            ...     filename="crs_full.parquet",
            ...     fetcher=lambda p: download_crs_to_path(p),
            ...     ttl_days=90
            ... )
            >>> path = manager.ensure(entry)
            >>> df = pd.read_parquet(path)
        """
        with self._lock:
            manifest = self._load_manifest()
            record = manifest.get(entry.key)
            path = self.base_dir / entry.filename

            # Check if we need to fetch
            needs_fetch = (
                refresh
                or record is None
                or not path.exists()
                or self._is_stale(record, entry)
            )

            if not needs_fetch:
                logger.info(f"Loading {entry.key} from cache")
                return path

            # Fetch fresh data
            logger.info(f"Fetching {entry.key} (refresh={refresh})")
            self._fetch_and_cache(entry, path)

            # Update manifest
            manifest[entry.key] = {
                "filename": entry.filename,
                "downloaded_at": datetime.now(timezone.utc).strftime(ISO_FORMAT),
                "ttl_days": entry.ttl_days,
                "version": entry.version,
            }
            self._save_manifest(manifest)

            return path

    def clear(self, key: Optional[str] = None) -> None:
        """Clear cached data.

        Args:
            key: If provided, clear only this entry. If None, clear all.

        Example:
            >>> manager.clear("crs_full")  # Clear specific entry
            >>> manager.clear()             # Clear all
        """
        with self._lock:
            manifest = self._load_manifest()

            if key is None:
                # Clear all
                for record in manifest.values():
                    file_path = self.base_dir / record["filename"]
                    file_path.unlink(missing_ok=True)
                manifest.clear()
                logger.info("Cleared all bulk cache entries")
            else:
                # Clear specific entry
                if key in manifest:
                    file_path = self.base_dir / manifest[key]["filename"]
                    file_path.unlink(missing_ok=True)
                    del manifest[key]
                    logger.info(f"Cleared cache entry: {key}")
                else:
                    logger.warning(f"Cache key not found: {key}")

            self._save_manifest(manifest)

    def list_records(self) -> list[dict[str, Any]]:
        """List all cached entries with metadata.

        Returns:
            List of dicts containing cache record information.

        Example:
            >>> for record in manager.list_records():
            ...     print(f"{record['key']}: {record['size_mb']:.1f} MB, "
            ...           f"age: {record['age_days']:.1f} days")
        """
        with self._lock:
            manifest = self._load_manifest()
            records = []

            for key, record in manifest.items():
                file_path = self.base_dir / record["filename"]
                size_mb = 0.0
                if file_path.exists():
                    size_mb = file_path.stat().st_size / (1024 * 1024)

                downloaded = datetime.strptime(record["downloaded_at"], ISO_FORMAT)
                age = datetime.now(timezone.utc) - downloaded

                records.append(
                    {
                        "key": key,
                        "filename": record["filename"],
                        "downloaded_at": record["downloaded_at"],
                        "age_days": age.days + age.seconds / 86400,
                        "ttl_days": record["ttl_days"],
                        "version": record.get("version"),
                        "size_mb": size_mb,
                        "is_stale": age.days > record["ttl_days"],
                    }
                )

            return records

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with total_entries, total_size_mb, stale_entries.

        Example:
            >>> stats = manager.stats()
            >>> print(f"Cache size: {stats['total_size_mb']:.1f} MB")
        """
        records = self.list_records()
        total_size = sum(r["size_mb"] for r in records)
        stale_count = sum(1 for r in records if r["is_stale"])

        return {
            "total_entries": len(records),
            "total_size_mb": total_size,
            "stale_entries": stale_count,
        }

    def _fetch_and_cache(self, entry: CacheEntry, path: Path) -> None:
        """Fetch data and write atomically.

        Uses temp-file-then-rename pattern to prevent corruption.
        """
        tmp_path = Path(f"{path}.tmp-{os.getpid()}")
        try:
            entry.fetcher(tmp_path)
            tmp_path.replace(path)  # Atomic rename
        finally:
            tmp_path.unlink(missing_ok=True)

    def _is_stale(self, record: dict, entry: CacheEntry) -> bool:
        """Check if cached entry is stale.

        An entry is stale if:
        1. Version changed (explicit cache bust)
        2. Age exceeds TTL
        """
        # Check version mismatch
        if entry.version is not None and entry.version != record.get("version"):
            return True

        # Check TTL expiration
        downloaded = datetime.strptime(record["downloaded_at"], ISO_FORMAT)
        age = datetime.now(timezone.utc) - downloaded
        ttl = timedelta(days=entry.ttl_days)

        return age > ttl

    def _load_manifest(self) -> dict:
        """Load manifest from disk."""
        if not self.manifest_path.exists():
            return {}

        try:
            with self.manifest_path.open("r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load manifest: {e}. Starting fresh.")
            return {}

    def _save_manifest(self, manifest: dict) -> None:
        """Save manifest to disk atomically."""
        tmp_path = Path(f"{self.manifest_path}.tmp-{os.getpid()}")
        try:
            with tmp_path.open("w") as f:
                json.dump(manifest, f, indent=2)
            tmp_path.replace(self.manifest_path)
        finally:
            tmp_path.unlink(missing_ok=True)


# Global singleton
_BULK_CACHE_MANAGER: Optional[CacheManager] = None


def bulk_cache_manager() -> CacheManager:
    """Get the global bulk cache manager singleton.

    Returns:
        CacheManager: The global cache manager instance.

    Example:
        >>> from oda_reader.cache_manager import bulk_cache_manager
        >>> manager = bulk_cache_manager()
        >>> stats = manager.stats()
    """
    global _BULK_CACHE_MANAGER
    if _BULK_CACHE_MANAGER is None:
        _BULK_CACHE_MANAGER = CacheManager()
    return _BULK_CACHE_MANAGER
