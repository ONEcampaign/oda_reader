"""File-based cache manager for bulk downloads.

Inspired by pydeflate's cache design, this module provides a robust caching system
for large, monolithic datasets (bulk CRS, Multisystem, AidData downloads).
"""

import json
import logging
import os
import shutil
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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
from oda_reader._cache.lease import derived_files, lease_guard, leased
from oda_reader.exceptions import validate_payload_or_raise

logger = logging.getLogger(__name__)

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
_TMP_MAX_AGE_SECONDS = 86_400  # 24 hours
_SNAPSHOT_PREFIX = ".snapshot-"


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
        release: Optional readable release id for the payload, such as the
            ``v20260803`` date OECD appends to a CRS annotation label.
            Recorded only when ``version`` is known, so it never describes a
            payload the version check could not confirm.
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
    release: str | None = None


class CacheManager:
    """Manages cached bulk files with a JSON manifest.

    Provides:
    - Atomic writes (temp file + rename, host+pid suffix for NFS safety)
    - Cross-process locking (FileLock)
    - TTL-based expiration
    - Version-based invalidation
    - Startup deletion of expired entries a newer copy of the dataset replaces
    - Observable cache state (list_records, stats)
    - Startup sweep of stale tmp files and snapshots

    Storage layout:
        {base_dir}/
        ├── manifest.json       # Metadata tracking
        ├── .cache.lock         # FileLock for coordination
        ├── .snapshot-*/        # Private read snapshots, removed after each read
        ├── {key}.zip           # Cached datasets
        └── {key}-*.parquet     # Files derived from a cached dataset, removed with it
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
        A hit fills in any of ``url``, ``flow_url`` and ``label`` the record
        lacks from ``entry``, so a record written before those were recorded
        becomes findable by ``find_entry``.

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
                missing = {
                    field: value
                    for field in ("url", "flow_url", "label")
                    if record.get(field) is None
                    and (value := getattr(entry, field)) is not None
                }
                if _confirmed_release(entry) not in (None, record.get("release")):
                    missing["release"] = entry.release
                if missing:
                    record.update(missing)
                    self._save_manifest(manifest)
                return path, record

            logger.info(f"Fetching {entry.key} (refresh={refresh})")
            # Validate only on fetch - trusting the manifest on hit avoids a
            # 10-30 s CRC walk per call on cached 976 MB zips.
            self._install(path, entry.fetcher)

            record = {
                "filename": entry.filename,
                "seq": _next_seq(manifest),
                "downloaded_at": datetime.now(UTC).strftime(ISO_FORMAT),
                "ttl_days": entry.ttl_days,
                "version": entry.version,
                "version_kind": entry.version_kind,
                "source": "download",
                "url": entry.url,
                "flow_url": entry.flow_url,
                "label": entry.label,
                "release": _confirmed_release(entry),
            }
            manifest[entry.key] = record
            self._save_manifest(manifest)

            return path, record

    @contextmanager
    def snapshot(
        self, entry: CacheEntry, *, refresh: bool = False
    ) -> Iterator[tuple[Path, dict[str, Any]]]:
        """Resolve *entry* as ``ensure_record`` does and yield a private copy of it.

        Under the cache lock, the cached payload is hard-linked into a private
        temporary directory inside the cache, or copied there where the
        filesystem refuses the link. Another process can then replace the
        cached file while the caller reads, and the snapshot still holds the
        bytes the record describes. The directory is removed on exit, and the
        startup sweep removes any a crashed process left behind.

        Args:
            entry: CacheEntry describing the dataset.
            refresh: If True, bypass cache and fetch fresh data.

        Yields:
            ``(snapshot_path, record)``: the private copy, named like the
            cached file, and the manifest record of the payload it holds.

        Raises:
            BulkPayloadCorruptError: As ``ensure``.
        """
        snapshot_dir = Path(
            tempfile.mkdtemp(prefix=_SNAPSHOT_PREFIX, dir=self.base_dir)
        )
        try:
            with self._lock:
                path, record = self.ensure_record(entry, refresh=refresh)
                snapshot_path = snapshot_dir / path.name
                try:
                    os.link(path, snapshot_path)
                except OSError:
                    shutil.copyfile(path, snapshot_path)
            yield snapshot_path, record
        finally:
            shutil.rmtree(snapshot_dir, ignore_errors=True)

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
            key: The manifest key, ``_bulk_cache_key(url)`` for bulk files.
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
                "seq": _next_seq(manifest),
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
            ``(key, path, url)`` of the entry ``find_record`` finds, or
            ``None``. ``url`` is ``None`` for an entry stored before URLs
            were recorded.
        """
        found = self.find_record(key=key, flow_url=flow_url, label=label)
        if found is None:
            return None
        entry_key, path, record = found
        return entry_key, path, record.get("url")

    def find_record(
        self,
        *,
        key: str | None = None,
        flow_url: str | None = None,
        label: str | None = None,
    ) -> tuple[str, Path, dict[str, Any]] | None:
        """Like ``find_entry``, and return the entry's manifest record.

        Args:
            key: The manifest key to match. When given, it alone decides
                the match and ``flow_url`` and ``label`` are ignored.
            flow_url: The dataflow URL to match, together with ``label``.
            label: The annotation label to match, together with ``flow_url``.

        Returns:
            ``(key, path, record)`` of the entry stored under ``key`` when a
            key is given, else of the last stored entry (see ``_recency``)
            matching ``flow_url`` and ``label``. ``None`` when no such
            entry's file exists.
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
            (entry_key, record)
            for entry_key, record in manifest.items()
            if matches_request(entry_key, record)
            and (self.base_dir / record["filename"]).exists()
        ]
        if not matches:
            return None
        entry_key, record = max(matches, key=lambda m: _recency(m[1]))
        return entry_key, self.base_dir / record["filename"], record

    def set_content_hash(self, key: str, content_hash: str, *, seq: int | None) -> bool:
        """Record *content_hash* on the entry ``key``, if it still holds payload ``seq``.

        Args:
            key: The manifest key.
            content_hash: The ``sha256:<hex>`` digest of the payload.
            seq: The ``seq`` of the record the caller hashed the payload of.
                A different ``seq`` means the payload was replaced in the
                meantime, and nothing is written.

        Returns:
            True if the hash was recorded.
        """
        with self._lock:
            manifest = self._load_manifest()
            record = manifest.get(key)
            if record is None or record.get("seq") != seq:
                return False
            record["content_hash"] = content_hash
            self._save_manifest(manifest)
            return True

    def link_payload(self, key: str, dest: Path, *, seq: int | None) -> bool:
        """Hard-link the payload of entry ``key`` to *dest*, if it still holds ``seq``.

        The check and the link run under the cache lock, so *dest* holds the
        bytes of the record with ``seq``. A later refetch replaces the cached
        file with a new inode and leaves *dest* as it was. Where the
        filesystem refuses the link, the payload is copied. *dest* appears
        complete or not at all.

        Args:
            key: The manifest key.
            dest: The new name for the payload, inside the cache directory.
            seq: The ``seq`` of the record the caller expects the payload of.

        Returns:
            True if *dest* was written; False if the entry is gone or holds
            another payload.
        """
        tmp_path = Path(f"{dest}.tmp-{_HOSTNAME}-{os.getpid()}")
        with self._lock:
            record = self._load_manifest().get(key)
            if record is None or record.get("seq") != seq:
                return False
            path = self.base_dir / record["filename"]
            try:
                try:
                    os.link(path, tmp_path)
                except OSError:
                    shutil.copyfile(path, tmp_path)
                tmp_path.replace(dest)
            finally:
                tmp_path.unlink(missing_ok=True)
            return True

    def clear(self, key: str | None = None) -> None:
        """Clear cached data.

        Clearing is explicit, so it also deletes Parquet files a `lease` holds.

        Args:
            key: If provided, clear only this entry. If None, clear all.
        """
        with self._lock:
            manifest = self._load_manifest()

            if key is None:
                for record in manifest.values():
                    self._remove_payload(record["filename"])
                manifest.clear()
                logger.info("Cleared all bulk cache entries")
            elif key in manifest:
                self._remove_payload(manifest[key]["filename"])
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
                        "flow_url": record.get("flow_url"),
                        "label": record.get("label"),
                        "release": record.get("release"),
                        "content_hash": record.get("content_hash"),
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

    def _remove_payload(self, filename: str) -> None:
        """Delete a cached payload and the ``{key}-*.parquet`` files derived from it."""
        path = self.base_dir / filename
        for derived in derived_files(path):
            derived.unlink(missing_ok=True)
        path.unlink(missing_ok=True)

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
        """Remove *.tmp-* files and snapshot directories older than 24 hours.

        A snapshot directory's mtime is when its payload was linked in; the
        payload's own mtime is the cached file's, so it says nothing about the
        snapshot's age.
        """
        now = datetime.now(UTC).timestamp()
        for tmp_file in self.base_dir.glob("*.tmp-*"):
            try:
                age = now - tmp_file.stat().st_mtime
                if age > _TMP_MAX_AGE_SECONDS:
                    tmp_file.unlink(missing_ok=True)
                    logger.info(f"Swept stale tmp file: {tmp_file}")
            except OSError as e:
                logger.warning(f"Could not inspect/remove tmp file {tmp_file}: {e}")
        for snapshot_dir in self.base_dir.glob(f"{_SNAPSHOT_PREFIX}*"):
            try:
                if now - snapshot_dir.stat().st_mtime > _TMP_MAX_AGE_SECONDS:
                    shutil.rmtree(snapshot_dir)
                    logger.info(f"Swept stale snapshot: {snapshot_dir}")
            except OSError as e:
                logger.warning(f"Could not inspect/remove snapshot {snapshot_dir}: {e}")

    def _evict_expired(self) -> None:
        """Delete expired downloads that a newer copy of the same dataset replaces.

        A downloaded entry goes, with its file, when it is past its own TTL and
        the manifest holds a later download or registration of the same
        dataset (same ``flow_url`` and ``label``) whose file exists, "later"
        as ``_recency`` ranks it. A dataset's newest copy stays, stale, so an
        online read refetches it and an offline read can still use it. Entries
        with no ``flow_url`` and ``label`` have no replacement, and registered
        entries have no TTL, so neither is deleted here. An entry with a derived file
        a `lease` holds stays, record and all, until a sweep finds it unleased.
        """
        manifest = self._load_manifest()
        now = datetime.now(UTC)
        try:
            newest: dict[tuple[str, str], tuple[int, datetime]] = {}
            for record in manifest.values():
                dataset = _dataset(record)
                if dataset is None or not (self.base_dir / record["filename"]).exists():
                    continue
                recency = _recency(record)
                newest[dataset] = max(newest.get(dataset, recency), recency)

            expired = []
            for key, record in manifest.items():
                dataset = _dataset(record)
                if _is_registered(record) or dataset is None:
                    continue
                recency = _recency(record)
                _, downloaded = recency
                if newest.get(dataset, recency) > recency and (
                    now - downloaded > timedelta(days=record["ttl_days"])
                ):
                    expired.append(key)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Expiry sweep skipped due to manifest parse error: {e}")
            return
        if not expired:
            return

        for key in expired:
            file_path = self.base_dir / manifest[key]["filename"]
            try:
                with lease_guard(file_path):
                    if any(leased(path) for path in derived_files(file_path)):
                        logger.info(f"Keeping expired {file_path}: a reader leases it.")
                        continue
                    self._remove_payload(manifest[key]["filename"])
            except OSError as e:
                # Keep the record so the next sweep retries the removal.
                logger.warning(f"Could not remove expired cache file {file_path}: {e}")
                continue
            logger.info(f"Expired cache entry removed: {file_path}")
            del manifest[key]

        self._save_manifest(manifest)


def _confirmed_release(entry: CacheEntry) -> str | None:
    """``entry.release`` when ``entry.version`` confirms which payload it names."""
    return entry.release if entry.version is not None else None


def _dataset(record: dict) -> tuple[str, str] | None:
    """The ``(flow_url, label)`` a record was resolved from, or None if unrecorded."""
    flow_url, label = record.get("flow_url"), record.get("label")
    if flow_url is None or label is None:
        return None
    return flow_url, label


def _next_seq(manifest: dict) -> int:
    """The ``seq`` for the next record written: one more than the largest so far."""
    return max((record.get("seq", 0) for record in manifest.values()), default=0) + 1


def _recency(record: dict) -> tuple[int, datetime]:
    """Rank a record by when it was written: ``seq``, then ``downloaded_at``.

    ``downloaded_at`` has one-second resolution, so two records written in the
    same second tie on it; ``seq`` orders them. A record written before ``seq``
    was recorded ranks as ``seq`` 0, below every record written since.
    """
    return record.get("seq", 0), datetime.strptime(record["downloaded_at"], ISO_FORMAT)


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
