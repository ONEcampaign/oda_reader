"""File-based cache manager for bulk downloads.

Inspired by pydeflate's cache design, this module provides a robust caching system
for large, monolithic datasets (bulk CRS, Multisystem, AidData downloads).
"""

import json
import logging
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from filelock import FileLock

from oda_reader._cache.config import (
    _HOSTNAME,
    get_bulk_cache_dir,
    register_cache_dir_change_listener,
)
from oda_reader.exceptions import validate_payload_or_raise

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
        ttl_days: Time-to-live in days (default: 90)
        version: Optional version string for cache invalidation
        version_kind: Optional name of the validator ``version`` holds, such
            as ``"etag"`` or ``"last_modified"``, recorded in the manifest
        url: Optional source URL, recorded in the manifest
        flow_url: Optional dataflow URL the bulk reader resolved ``url``
            from, recorded so ``find_entry`` can match it offline
        label: Optional EXT_RESOURCE annotation label within ``flow_url``
    """

    key: str
    filename: str
    fetcher: Callable[[Path], None]
    ttl_days: int = 90
    version: str | None = None
    version_kind: str | None = None
    url: str | None = None
    flow_url: str | None = None
    label: str | None = None


class CacheManager:
    """Manages cached bulk files with a JSON manifest.

    Provides:
    - Atomic writes (temp file + rename, host+pid suffix for NFS safety)
    - Cross-process locking (FileLock)
    - TTL-based expiration
    - Version-based invalidation
    - Startup deletion of entries past their TTL
    - Observable cache state (list_records, stats)
    - Startup sweep of stale tmp files

    Storage layout:
        {base_dir}/
        ├── manifest.json       # Metadata tracking
        ├── .cache.lock         # FileLock for coordination
        └── *.zip               # Cached datasets
    """

    def __init__(self, base_dir: Path | None = None) -> None:
        """Initialize cache manager.

        Args:
            base_dir: Directory for cache storage. If None, uses get_bulk_cache_dir().
        """
        self.base_dir = base_dir or get_bulk_cache_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.manifest_path = self.base_dir / "manifest.json"
        self.lock_path = self.base_dir / ".cache.lock"
        self._lock = FileLock(str(self.lock_path), timeout=1200)

        # Best-effort startup maintenance. If another process holds the lock
        # (e.g., mid-fetch), skip — it'll run on the next clean startup.
        try:
            with self._lock.acquire(timeout=0):
                self._sweep_stale_tmp_files()
                self._evict_expired()
        except Exception as e:
            logger.debug(f"Skipping startup cache maintenance: {e}")

    def ensure(self, entry: CacheEntry, refresh: bool = False) -> Path:
        """Ensure cached file exists, fetching if necessary.

        On a cache-miss or refresh, the download is written to a temporary
        file and validated with ``validate_payload_or_raise``, which
        dispatches on magic bytes: a zip integrity walk (``is_zipfile`` +
        ``testzip``) for a zip payload, or a footer-magic and metadata check
        for a bare parquet payload. Only a payload that passes replaces the
        cached file and its manifest record. On a cache-hit the manifest
        record is trusted (no walk on every call).

        Args:
            entry: CacheEntry describing the dataset.
            refresh: If True, bypass cache and fetch fresh data.

        Returns:
            Path: Path to the cached file. On cache-miss / refetch the file is
                guaranteed to have passed ``validate_payload_or_raise`` for its
                detected type. On cache-hit the manifest record is trusted.

        Raises:
            BulkPayloadCorruptError: If a freshly downloaded file fails
                integrity validation. Any previously cached file and its
                manifest record are left in place.
        """
        path, _ = self.ensure_record(entry, refresh=refresh)
        return path

    def ensure_record(
        self, entry: CacheEntry, *, refresh: bool = False
    ) -> tuple[Path, dict[str, Any]]:
        """Like ``ensure``, and also return the manifest record of the file served.

        The record describes the payload at the returned path, so a cache hit
        reports the version stored with that payload, not ``entry.version``.

        Returns:
            ``(path, record)``, read under one lock acquisition.
        """
        with self._lock:
            manifest = self._load_manifest()
            record = manifest.get(entry.key)
            path = self.base_dir / entry.filename

            if (
                not refresh
                and record is not None
                and path.exists()
                and not self._is_stale(record, entry)
            ):
                logger.info(f"Loading {entry.key} from cache")
                return path, record

            logger.info(f"Fetching {entry.key} (refresh={refresh})")
            # Validate only on fetch - trusting the manifest on hit avoids a
            # 10-30 s CRC walk per call on cached 976 MB zips.
            self._install(path, entry.fetcher)

            record = {
                "filename": entry.filename,
                "downloaded_at": datetime.now(UTC).strftime(ISO_FORMAT),
                "ttl_days": entry.ttl_days,
                "version": entry.version,
                "version_kind": entry.version_kind,
                "source": "download",
                "url": entry.url,
                "flow_url": entry.flow_url,
                "label": entry.label,
            }
            manifest[entry.key] = record
            self._save_manifest(manifest)

            return path, record

    def register(
        self,
        source: Path,
        *,
        key: str,
        filename: str,
        url: str,
        version: str | None = None,
        flow_url: str | None = None,
        label: str | None = None,
    ) -> Path:
        """Copy a local file into the cache as ``key``, replacing any entry there.

        The copy is validated with ``validate_payload_or_raise`` before it
        replaces the cached file, so a bad payload leaves both ``source`` and
        any existing entry untouched.

        A registered entry has no TTL, so the expiry sweep never deletes it. It
        is replaced only by another registration, by an ``ensure`` that fetches
        the same key (after a version change or with ``refresh``), or by
        ``clear``.

        Args:
            source: The file to copy in. It is never modified.
            key: The manifest key, ``sha1(url)`` for bulk files.
            filename: The storage filename inside the cache directory.
            url: The URL the file was downloaded from.
            version: The cache-invalidation token to store. A later
                ``ensure`` with a different non-``None`` version refetches.
            flow_url: The dataflow URL the bulk reader resolves ``url`` from.
            label: The EXT_RESOURCE annotation label within ``flow_url``.

        Returns:
            Path: Path to the cached copy.

        Raises:
            BulkPayloadCorruptError: If the copy fails integrity validation.
        """
        with self._lock:
            path = self.base_dir / filename
            self._install(path, lambda tmp_path: shutil.copyfile(source, tmp_path))

            manifest = self._load_manifest()
            manifest[key] = {
                "filename": filename,
                "downloaded_at": datetime.now(UTC).strftime(ISO_FORMAT),
                "ttl_days": None,
                "version": version,
                "source": "registered",
                "url": url,
                "flow_url": flow_url,
                "label": label,
            }
            self._save_manifest(manifest)

        logger.info(f"Registered {source} as {key}")
        return path

    def find_entry(
        self,
        *,
        key: str | None = None,
        flow_url: str | None = None,
        label: str | None = None,
    ) -> tuple[str, Path, str | None] | None:
        """Find a cached file by key or by dataflow annotation, whatever its age.

        Offline reads use it to find a cached copy of any age.

        Args:
            key: The manifest key to match. When given, it alone decides
                the match and ``flow_url`` and ``label`` are ignored.
            flow_url: The dataflow URL to match, together with ``label``.
            label: The annotation label to match, together with ``flow_url``.

        Returns:
            ``(key, path, url)`` of the entry stored under ``key`` when a key
            is given, else of the most recently stored entry matching
            ``flow_url`` and ``label``. ``None`` when no such entry's file
            exists. ``url`` is ``None`` for an entry stored before URLs were
            recorded.
        """
        with self._lock:
            manifest = self._load_manifest()

        def matches_request(entry_key: str, record: dict) -> bool:
            if key is not None:
                return entry_key == key
            return (
                flow_url is not None
                and record.get("flow_url") == flow_url
                and record.get("label") == label
            )

        matches = [
            (datetime.strptime(record["downloaded_at"], ISO_FORMAT), entry_key, record)
            for entry_key, record in manifest.items()
            if matches_request(entry_key, record)
            and (self.base_dir / record["filename"]).exists()
        ]
        if not matches:
            return None
        _, entry_key, record = max(matches, key=lambda m: m[0])
        return entry_key, self.base_dir / record["filename"], record.get("url")

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
                age = datetime.now(UTC) - downloaded

                records.append(
                    {
                        "key": key,
                        "filename": record["filename"],
                        "downloaded_at": record["downloaded_at"],
                        "age_days": age.days + age.seconds / 86400,
                        "ttl_days": record["ttl_days"],
                        "version": record.get("version"),
                        "source": record.get("source", "download"),
                        "url": record.get("url"),
                        "size_mb": size_mb,
                        "is_stale": not _is_registered(record)
                        and age.days > record["ttl_days"],
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

    def _install(self, path: Path, write: Callable[[Path], object]) -> None:
        """Write a payload to a host+pid tmp file, validate it, then move it to *path*.

        A payload that fails validation never reaches *path*, so the file
        already there survives a failed download or registration.
        """
        tmp_path = Path(f"{path}.tmp-{_HOSTNAME}-{os.getpid()}")
        try:
            write(tmp_path)
            validate_payload_or_raise(tmp_path)
            tmp_path.replace(path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _is_stale(self, record: dict, entry: CacheEntry) -> bool:
        """Check if cached entry is stale.

        An entry is stale if:
        1. Version changed (explicit cache bust)
        2. Age exceeds TTL, unless the entry was registered
        """
        if entry.version is not None and entry.version != record.get("version"):
            return True

        if _is_registered(record):
            return False

        downloaded = datetime.strptime(record["downloaded_at"], ISO_FORMAT)
        age = datetime.now(UTC) - downloaded
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
        now = datetime.now(UTC).timestamp()
        for tmp_file in self.base_dir.glob("*.tmp-*"):
            try:
                age = now - tmp_file.stat().st_mtime
                if age > _TMP_MAX_AGE_SECONDS:
                    tmp_file.unlink(missing_ok=True)
                    logger.info(f"Swept stale tmp file: {tmp_file}")
            except OSError as e:
                logger.warning(f"Could not inspect/remove tmp file {tmp_file}: {e}")

    def _evict_expired(self) -> None:
        """Delete every entry older than its own TTL, along with its file.

        Each URL has exactly one entry, and a refetch overwrites that entry's
        file in place, so the TTL is the only bound on what the cache holds.
        Registered entries have no TTL and are never deleted here.
        """
        manifest = self._load_manifest()
        now = datetime.now(UTC)
        try:
            expired = [
                key
                for key, record in manifest.items()
                if not _is_registered(record)
                and now - datetime.strptime(record["downloaded_at"], ISO_FORMAT)
                > timedelta(days=record["ttl_days"])
            ]
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Expiry sweep skipped due to manifest parse error: {e}")
            return
        if not expired:
            return

        for key in expired:
            file_path = self.base_dir / manifest[key]["filename"]
            try:
                file_path.unlink(missing_ok=True)
            except OSError as e:
                # Keep the record so the next sweep retries the removal.
                logger.warning(f"Could not remove expired cache file {file_path}: {e}")
                continue
            logger.info(f"Expired cache entry removed: {file_path}")
            del manifest[key]

        self._save_manifest(manifest)


def _is_registered(record: dict) -> bool:
    """True for a manifest record written by ``CacheManager.register``."""
    return record.get("source") == "registered"


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
