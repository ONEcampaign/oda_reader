"""File-based cache manager for bulk downloads.

Inspired by pydeflate's cache design, this module provides a robust caching system
for large, monolithic datasets (bulk CRS, Multisystem, AidData downloads).
"""

import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from filelock import FileLock

from oda_reader._cache.config import (
    _HOSTNAME,
    get_bulk_cache_dir,
    register_cache_dir_change_listener,
)
from oda_reader.exceptions import validate_zip_or_raise

logger = logging.getLogger("oda_reader")

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
_TMP_MAX_AGE_SECONDS = 86_400  # 24 hours


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
    version: str | None = None


class CacheManager:
    """Manages cached bulk files with a JSON manifest.

    Provides:
    - Atomic writes (temp file + rename, host+pid suffix for NFS safety)
    - Cross-process locking (FileLock)
    - TTL-based expiration
    - Version-based invalidation
    - LRU eviction per dataset key prefix (keep_n most-recent)
    - Observable cache state (list_records, stats)
    - Startup sweep of stale tmp files

    Storage layout:
        {base_dir}/
        ├── manifest.json       # Metadata tracking
        ├── .cache.lock         # FileLock for coordination
        └── *.zip               # Cached datasets
    """

    def __init__(self, base_dir: Path | None = None, *, keep_n: int = 2) -> None:
        """Initialize cache manager.

        Args:
            base_dir: Directory for cache storage. If None, uses get_bulk_cache_dir().
            keep_n: Maximum number of entries to retain per dataset key prefix.
                Oldest entries beyond this limit are evicted on init.
        """
        self.base_dir = base_dir or get_bulk_cache_dir()
        self.keep_n = keep_n
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.manifest_path = self.base_dir / "manifest.json"
        self.lock_path = self.base_dir / ".cache.lock"
        self._lock = FileLock(str(self.lock_path), timeout=1200)

        # Best-effort startup maintenance. If another process holds the lock
        # (e.g., mid-fetch), skip — it'll run on the next clean startup.
        try:
            with self._lock.acquire(timeout=0):
                self._sweep_stale_tmp_files()
                self._evict_lru()
        except Exception as e:
            logger.debug(f"Skipping startup cache maintenance: {e}")

    def ensure(self, entry: CacheEntry, refresh: bool = False) -> Path:
        """Ensure cached file exists, fetching if necessary.

        On a cache-miss or refresh, the downloaded file is validated with
        ``is_zipfile`` and ``testzip``. On a cache-hit the manifest record is
        trusted (no CRC walk on every call).

        Args:
            entry: CacheEntry describing the dataset.
            refresh: If True, bypass cache and fetch fresh data.

        Returns:
            Path: Path to the cached file. On cache-miss / refetch the file is
                guaranteed to satisfy ``is_zipfile(path) and
                ZipFile(path).testzip() is None``. On cache-hit the manifest
                record is trusted.

        Raises:
            BulkPayloadCorruptError: If a freshly downloaded file fails
                integrity validation. The cache entry is removed before raising.
        """
        with self._lock:
            manifest = self._load_manifest()
            record = manifest.get(entry.key)
            path = self.base_dir / entry.filename

            needs_fetch = (
                refresh
                or record is None
                or not path.exists()
                or self._is_stale(record, entry)
            )

            if not needs_fetch:
                logger.info(f"Loading {entry.key} from cache")
                return path

            logger.info(f"Fetching {entry.key} (refresh={refresh})")
            self._fetch_and_cache(entry, path)

            # Validate only on fetch - trusting the manifest on hit avoids a
            # 10-30 s CRC walk per call on cached 976 MB zips.
            validate_zip_or_raise(path)

            manifest[entry.key] = {
                "filename": entry.filename,
                "downloaded_at": datetime.now(timezone.utc).strftime(ISO_FORMAT),
                "ttl_days": entry.ttl_days,
                "version": entry.version,
            }
            self._save_manifest(manifest)

            return path

    def clear(self, key: str | None = None) -> None:
        """Clear cached data.

        Args:
            key: If provided, clear only this entry. If None, clear all.
        """
        with self._lock:
            manifest = self._load_manifest()

            if key is None:
                for record in manifest.values():
                    file_path = self.base_dir / record["filename"]
                    file_path.unlink(missing_ok=True)
                manifest.clear()
                logger.info("Cleared all bulk cache entries")
            elif key in manifest:
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
        """Fetch data and write atomically using host+pid tmp suffix."""
        tmp_path = Path(f"{path}.tmp-{_HOSTNAME}-{os.getpid()}")
        try:
            entry.fetcher(tmp_path)
            tmp_path.replace(path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _is_stale(self, record: dict, entry: CacheEntry) -> bool:
        """Check if cached entry is stale.

        An entry is stale if:
        1. Version changed (explicit cache bust)
        2. Age exceeds TTL
        """
        if entry.version is not None and entry.version != record.get("version"):
            return True

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
        """Save manifest to disk atomically using host+pid tmp suffix."""
        tmp_path = Path(f"{self.manifest_path}.tmp-{_HOSTNAME}-{os.getpid()}")
        try:
            with tmp_path.open("w") as f:
                json.dump(manifest, f, indent=2)
            tmp_path.replace(self.manifest_path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _sweep_stale_tmp_files(self) -> None:
        """Remove *.tmp-* files in base_dir that are older than 24 hours."""
        now = datetime.now(timezone.utc).timestamp()
        for tmp_file in self.base_dir.glob("*.tmp-*"):
            try:
                age = now - tmp_file.stat().st_mtime
                if age > _TMP_MAX_AGE_SECONDS:
                    tmp_file.unlink(missing_ok=True)
                    logger.info(f"Swept stale tmp file: {tmp_file}")
            except OSError as e:
                logger.warning(f"Could not inspect/remove tmp file {tmp_file}: {e}")

    def _evict_lru(self) -> None:
        """Evict oldest entries beyond keep_n per dataset key prefix.

        A "key prefix" is the full manifest key (e.g., ``sha1_hexdigest``).
        Since all entries in the bulk cache share the same key space and each
        URL produces a unique sha1 key, LRU is applied across *all* entries
        collectively: keep the ``keep_n`` most-recently-downloaded, evict the
        rest.
        """
        manifest = self._load_manifest()
        if len(manifest) <= self.keep_n:
            return

        try:
            ordered = sorted(
                manifest.items(),
                key=lambda kv: datetime.strptime(kv[1]["downloaded_at"], ISO_FORMAT),
            )
        except (KeyError, ValueError) as e:
            logger.warning(f"LRU eviction skipped due to manifest parse error: {e}")
            return

        to_evict = ordered[: len(ordered) - self.keep_n]
        for key, record in to_evict:
            file_path = self.base_dir / record["filename"]
            try:
                file_path.unlink(missing_ok=True)
                logger.info(f"LRU eviction: removed {file_path}")
            except OSError as e:
                logger.warning(f"LRU eviction: could not remove {file_path}: {e}")
            del manifest[key]

        self._save_manifest(manifest)


_BULK_CACHE_MANAGER: CacheManager | None = None


def bulk_cache_manager() -> CacheManager:
    """Get the global bulk cache manager singleton.

    Returns:
        CacheManager: The global cache manager instance.
    """
    global _BULK_CACHE_MANAGER
    if _BULK_CACHE_MANAGER is None:
        _BULK_CACHE_MANAGER = CacheManager()
    return _BULK_CACHE_MANAGER


def _reset_bulk_cache_manager() -> None:
    """Reset the singleton so the next access rebuilds against the current cache dir."""
    global _BULK_CACHE_MANAGER
    _BULK_CACHE_MANAGER = None


register_cache_dir_change_listener(_reset_bulk_cache_manager)
