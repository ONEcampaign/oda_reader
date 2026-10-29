"""Local Parquet tables for the DAC1, DAC2A, DAC2B, CRS and Multisystem files.

`scan` serves each bulk file as one Parquet file on disk that a query engine
such as DuckDB reads in place. The file is ``{key}-{digest}.parquet`` beside
the cached bulk file, where ``digest`` hashes the release token, so its bytes
never change while it exists. CRS is published as Parquet, and its file is a
hard link to the cached payload. DAC1, DAC2A, DAC2B and Multisystem are
published as zipped CSVs, and are converted once per release. The files of
the current and the previous release are kept until the cache is cleared, even
when OECD moves the dataset to a new URL, and older ones are deleted unless a
reader holds a lease on them.

`lease` scans like `scan` and holds the file until the lease is closed; see
`oda_reader._cache.lease` for how a lease works.
"""

import contextlib
import hashlib
import io
import logging
import os
import typing
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import TracebackType

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from filelock import FileLock

from oda_reader._cache.config import get_bulk_cache_dir
from oda_reader._cache.lease import release_lease, take_lease
from oda_reader._cache.manager import (
    ISO_FORMAT,
    CacheManager,
    _payload_id,
    bulk_cache_manager,
    derived_files,
    stale_marker,
)
from oda_reader.download.download_tools import (
    BULK_DATASETS,
    _atomic_write,
    _bulk_cache_key,
    _bulk_dataset_name,
    _detect_delimiter,
    _ensure_cached_bulk_file,
    _fetch_revalidation_token,
    _resolve_bulk_file,
)
from oda_reader.exceptions import BulkDownloadHTTPError, BulkFileNotCachedError
from oda_reader.schemas.bulk_columns import (
    DOTSTAT_TABLE_COLUMNS,
    MULTISYSTEM_COLUMNS,
    BulkColumn,
    converted_columns,
)

logger = logging.getLogger(__name__)

type ScanDataset = typing.Literal["dac1", "dac2a", "dac2b", "crs", "multisystem"]

# The converted file's row-group size. DuckDB writes row groups of 122,880
# rows and skips any whose min/max statistics exclude a filter. The bulk CSVs
# are sorted by year, so a year filter skips most groups; other columns span
# their whole range in every group.
_ROW_GROUP_ROWS = 122_880

# Columns per dataset for the zipped-CSV datasets. CRS is served as published.
_CONVERTED_COLUMNS: dict[str, Mapping[str, BulkColumn | None]] = {
    "dac1": DOTSTAT_TABLE_COLUMNS,
    "dac2a": DOTSTAT_TABLE_COLUMNS,
    "dac2b": DOTSTAT_TABLE_COLUMNS,
    "multisystem": MULTISYSTEM_COLUMNS,
}
_SCAN_DATASETS = ("dac1", "dac2a", "dac2b", "crs", "multisystem")


@dataclass(frozen=True)
class ScannedTable:
    """A bulk dataset as one local Parquet file.

    Attributes:
        path: The Parquet file. Read it in place; do not modify it. Its bytes
            stay those of `release` while it exists, and it is kept until
            a second newer release has been scanned or the cache is cleared.
            A `TableLease` on it keeps it past newer releases while it is open.
        release: The release token of the bulk file it holds. See `release`.
        label_release: The readable release id OECD appends to the dataset's
            annotation label, such as ``"v20260803"`` for CRS, or ``None``
            when the label carries none or the host gave no token to confirm
            it describes these bytes.
        downloaded: When the bulk file was downloaded or registered (UTC).
        columns: The Parquet file's column names, in file order.
    """

    path: Path
    release: str
    label_release: str | None
    downloaded: datetime
    columns: tuple[str, ...]


@dataclass(frozen=True)
class BulkFileInfo:
    """One file in the bulk file cache.

    Attributes:
        dataset: The `register_bulk_file` dataset name (``"crs"``,
            ``"dac1"`` and so on), or ``None`` for a file without one, such
            as a CRS year file.
        release: The file's release token, or ``None`` when none is
            recorded yet. See `release`.
        label_release: The readable release id of the annotation label, as
            in `ScannedTable`, or ``None``.
        downloaded: When the file was downloaded or registered (UTC).
        size_bytes: The file's size on disk, 0 if it is missing.
        derived_bytes: The disk space used by the Parquet files `scan`
            made from it, counting a hard link to the file as 0.
        stale: True when a downloaded file is past its time-to-live. An
            online read of its URL downloads it again, and an offline read
            still uses it. A file kept for the previous release after OECD
            moved the dataset to a new URL stays stale and is not downloaded
            again.
        path: The cached file.
    """

    dataset: str | None
    release: str | None
    label_release: str | None
    downloaded: datetime
    size_bytes: int
    derived_bytes: int
    stale: bool
    path: Path


class TableLease:
    """A scanned table whose Parquet file is kept until the lease is closed.

    `lease` returns it. While it is open, no later `scan` or `lease` deletes
    `table.path`, so a reader that keeps the path, such as a DuckDB view,
    can read it for as long as it needs. Close it when the reader is done, or
    use it as a context manager. A process that exits releases its leases.
    Clearing the cache deletes the file all the same.

    The lease lasts only while this object is kept open. One that is dropped
    without `close`, as in ``oda_reader.lease(x).table.path``, is released at
    once. Hold the object or use a ``with`` block for as long as the path is
    read.

    Attributes:
        table: The scanned table, as `scan` returns it.
    """

    def __init__(self, table: ScannedTable, lock: FileLock) -> None:
        self.table = table
        self._lock = lock

    def close(self) -> None:
        """Release the lease. Closing it again does nothing."""
        if self._lock.is_locked:
            release_lease(self._lock)

    def __enter__(self) -> typing.Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def scan(
    dataset: ScanDataset,
    *,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
    offline: bool = False,
) -> ScannedTable:
    """Return a bulk dataset as one local Parquet file, downloading it if needed.

    CRS is served as published, through a hard link to the cached file. DAC1,
    DAC2A, DAC2B and Multisystem are converted from their zipped CSV to Parquet the
    first time a release is scanned, and the converted file is reused until
    the release token changes. The returned path is named for its release, so
    a later download or conversion leaves its bytes unchanged. The files of
    the current and the previous release are kept until the cache is
    cleared, also across a change of the dataset's URL, and older ones are
    deleted, except those a `lease` holds.
    Converted columns are snake_case and unique
    case-insensitively (``DONOR`` and ``Donor`` become ``donor_code`` and
    ``donor_name``), so DuckDB reads them without renaming.

    Args:
        dataset: ``"dac1"``, ``"dac2a"``, ``"dac2b"``, ``"crs"`` or
            ``"multisystem"``.
        cache_dir: A cache root for this call only. The bulk file and its
            Parquet conversion are read from and written to it, and the
            process-wide cache root is left unchanged. The dataflow lookup
            that resolves the file's URL still uses the process-wide HTTP
            cache.
        refresh: If True, download the bulk file even when a current copy is
            cached, and bypass the cached dataflow annotations.
        offline: If True, make no network request and use the cached or
            registered copy whatever its age.

    Returns:
        ScannedTable: The Parquet file, its release token and readable label
            release, download time and column names.

    Raises:
        ValueError: If `dataset` is not one of the four names, or if both
            `refresh` and `offline` are True.
        BulkFileNotCachedError: If `offline` is True and the cache holds no
            copy.
        BulkDownloadChallengeError: If the file host challenges the download.
            Register a browser download with `register_bulk_file`.
        RuntimeError: If another process replaced or cleared the cached bulk
            file while it was being served. Scanning again serves the new file.
    """
    manager, key, record, token = _resolve(
        dataset, cache_dir=cache_dir, refresh=refresh, offline=offline
    )
    return _scanned_table(dataset, manager, key, record, token)


def lease(
    dataset: ScanDataset,
    *,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
    offline: bool = False,
) -> TableLease:
    """Scan a bulk dataset as `scan` does, and keep its Parquet file until closed.

    `scan` deletes the files of releases before the previous one, which
    breaks a reader that still holds an older path. A lease keeps its file
    through any number of newer releases until `TableLease.close`. Each
    lease is an exclusive lock on a file of its own, so leases from any
    number of threads and processes coexist, and one that a process leaves
    open is released when the process exits. Clearing the cache deletes a
    leased file.

    Use a ``with`` block or hold the returned object for as long as the path
    is read; see `TableLease`.

    Args:
        dataset: ``"dac1"``, ``"dac2a"``, ``"dac2b"``, ``"crs"`` or
            ``"multisystem"``.
        cache_dir: A cache root for this call only, as in `scan`.
        refresh: If True, download the bulk file even when a current copy is
            cached, as in `scan`.
        offline: If True, make no network request, as in `scan`.

    Returns:
        TableLease: The open lease. Its `table` is the `ScannedTable`.

    Raises:
        The errors `scan` raises, with no lease left held.
    """
    manager, key, record, token = _resolve(
        dataset, cache_dir=cache_dir, refresh=refresh, offline=offline
    )
    # The lease comes before the file is looked up, so no prune can delete
    # the file between the lookup and the caller's first read.
    lock = take_lease(_table_path(manager.base_dir / record["filename"], token))
    try:
        table = _scanned_table(dataset, manager, key, record, token)
    except BaseException:
        release_lease(lock)
        raise
    return TableLease(table, lock)


def release(
    dataset: ScanDataset,
    *,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
    offline: bool = False,
) -> str:
    """Return the release token of a bulk dataset.

    The token is the file host's ETag, else its Last-Modified date, for every
    dataset. Two copies of the same published file have the same token,
    whenever and wherever they were downloaded, and a file OECD rewrites gets
    a new one. A cached file with neither (a file registered while the host
    was unreachable) gets ``"sha256:<digest of the file>"``. A download
    records the token its own response carried, so a downloaded copy's token
    names the bytes it holds even if OECD republished during the download.
    The readable CRS id, such as ``"v20260803"``, is
    `ScannedTable.label_release`.

    Online, the token describes the file OECD publishes now, read with one
    request for the dataflow annotations and a one-byte request to the file
    host. Nothing is downloaded. When the host sends no token, the cached
    copy's token is returned, as `scan` reports it.

    Args:
        dataset: ``"dac1"``, ``"dac2a"``, ``"dac2b"``, ``"crs"`` or
            ``"multisystem"``.
        cache_dir: A cache root for this call only, as in `scan`.
        refresh: If True, bypass the cached dataflow annotations.
        offline: If True, make no network request and return the token of
            the cached or registered copy.

    Returns:
        str: The release token.

    Raises:
        ValueError: If `dataset` is not one of the four names, or if both
            `refresh` and `offline` are True.
        BulkFileNotCachedError: If the token comes from the cache (offline,
            or online when the host sends none) and the cache holds no copy.
    """
    _check_dataset(dataset)
    _check_modes(refresh=refresh, offline=offline)
    manager = _manager(cache_dir)
    flow_url, label = BULK_DATASETS[dataset]

    url = None
    if not offline:
        url, _label_release = _resolve_bulk_file(
            flow_url, label, bypass_annotation_cache=refresh
        )
        if (version := _fetch_revalidation_token(url)) is not None:
            return version

    found = manager.find_record(flow_url=flow_url, label=label)
    if found is None:
        raise BulkFileNotCachedError(dataset=dataset, url=url)
    return _record_release(manager, *found)


def cache_info(*, cache_dir: str | Path | None = None) -> list[BulkFileInfo]:
    """List the files in the bulk file cache.

    The Parquet files `scan` makes are not listed; each entry's
    `derived_bytes` counts them. They sit beside the bulk file they were
    made from and are deleted with it.

    Args:
        cache_dir: A cache root for this call only, as in `scan`.

    Returns:
        list[BulkFileInfo]: One entry per cached file, in manifest order.
    """
    manager = _manager(cache_dir)
    infos = []
    for row in manager.list_records():
        path = manager.base_dir / row["filename"]
        stat = path.stat() if path.exists() else None
        infos.append(
            BulkFileInfo(
                dataset=_bulk_dataset_name(row["flow_url"], row["label"]),
                release=row["version"] or row["content_hash"],
                label_release=row["release"],
                downloaded=_downloaded_at(row),
                size_bytes=stat.st_size if stat else 0,
                derived_bytes=_derived_bytes(path, stat),
                stale=row["is_stale"],
                path=path,
            )
        )
    return infos


def _resolve(
    dataset: str, *, cache_dir: str | Path | None, refresh: bool, offline: bool
) -> tuple[CacheManager, str, dict[str, typing.Any], str]:
    """Cache *dataset*'s bulk file; return its manager, key, record and token."""
    _check_dataset(dataset)
    manager = _manager(cache_dir)
    key, path, record = _bulk_file(dataset, manager, refresh=refresh, offline=offline)
    return manager, key, record, _record_release(manager, key, path, record)


def _scanned_table(
    dataset: str,
    manager: CacheManager,
    key: str,
    record: Mapping[str, typing.Any],
    token: str,
) -> ScannedTable:
    table_path = _table_file(
        manager, key, record, token, _CONVERTED_COLUMNS.get(dataset)
    )
    return ScannedTable(
        path=table_path,
        release=token,
        label_release=record.get("release"),
        downloaded=_downloaded_at(record),
        columns=tuple(pq.read_schema(table_path).names),
    )


def _check_dataset(dataset: str) -> None:
    if dataset not in _SCAN_DATASETS:
        known = ", ".join(repr(name) for name in _SCAN_DATASETS)
        raise ValueError(f"Unknown dataset {dataset!r}. Use one of {known}.")


def _check_modes(*, refresh: bool, offline: bool) -> None:
    if refresh and offline:
        raise ValueError("refresh=True downloads; it cannot be combined with offline.")


def _manager(cache_dir: str | Path | None) -> CacheManager:
    """The process-wide bulk cache, or a manager scoped to *cache_dir*."""
    if cache_dir is None:
        return bulk_cache_manager()
    return CacheManager(get_bulk_cache_dir(cache_dir))


def _bulk_file(
    dataset: str, manager: CacheManager, *, refresh: bool, offline: bool
) -> tuple[str, Path, dict[str, typing.Any]]:
    """Resolve *dataset* to ``(key, path, record)`` in *manager*'s cache."""
    _check_modes(refresh=refresh, offline=offline)
    flow_url, label = BULK_DATASETS[dataset]

    if offline:
        found = manager.find_record(flow_url=flow_url, label=label)
        if found is None:
            raise BulkFileNotCachedError(dataset=dataset, url=None)
        return found

    def fetch(*, bypass_annotation_cache: bool) -> tuple[str, Path, dict]:
        url, label_release = _resolve_bulk_file(
            flow_url, label, bypass_annotation_cache=bypass_annotation_cache
        )
        path, record = _ensure_cached_bulk_file(
            url,
            _fetch_revalidation_token(url),
            flow_url=flow_url,
            label=label,
            manager=manager,
            release=label_release if dataset == "crs" else None,
            refresh=refresh,
        )
        return _bulk_cache_key(url), path, record

    try:
        return fetch(bypass_annotation_cache=refresh)
    except BulkDownloadHTTPError as e:
        # A 404 means the cached annotation points at a retired URL.
        if e.status_code != 404 or refresh:
            raise
        logger.info(f"Bulk file for {dataset} returned 404; re-resolving once.")
        return fetch(bypass_annotation_cache=True)


def _record_release(
    manager: CacheManager, key: str, path: Path, record: Mapping[str, typing.Any]
) -> str:
    """The release token of a cached payload, hashing the file when none is recorded."""
    if token := record.get("version") or record.get("content_hash"):
        return token
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    token = f"sha256:{digest.hexdigest()}"
    manager.set_content_hash(key, token, payload_id=_payload_id(record))
    return token


def _downloaded_at(record: Mapping[str, typing.Any]) -> datetime:
    return datetime.strptime(record["downloaded_at"], ISO_FORMAT)


def _derived_bytes(payload: Path, stat: os.stat_result | None) -> int:
    """Disk used by *payload*'s derived files, skipping hard links to it."""
    total = 0
    for derived in derived_files(payload):
        with contextlib.suppress(OSError):
            derived_stat = derived.stat()
            if stat is None or not os.path.samestat(derived_stat, stat):
                total += derived_stat.st_size
    return total


def _table_file(
    manager: CacheManager,
    key: str,
    record: Mapping[str, typing.Any],
    token: str,
    columns: Mapping[str, BulkColumn | None] | None,
) -> Path:
    """The Parquet file for the cached payload *key* at release *token*.

    A Parquet payload (*columns* is ``None``) is hard-linked to the file; a
    zipped CSV is converted into it with *columns*. Either is done once,
    under a lock so concurrent scans do it once, and then removes files of
    releases before the previous one. A file marked stale, which a lease
    kept after its payload was discarded as corrupt, is made again.

    Raises:
        RuntimeError: If another process replaced the cached payload after
            *record* was read, so *token* no longer names its contents, or
            cleared it during the conversion.
    """
    payload = manager.base_dir / record["filename"]
    dest = _table_path(payload, token)
    stale = stale_marker(dest)
    if dest.exists() and not stale.exists():
        return dest

    with FileLock(str(payload.with_name(f".{payload.stem}.convert.lock"))):
        if dest.exists() and not stale.exists():
            return dest
        if columns is None:
            if not manager.link_payload(key, dest, payload_id=_payload_id(record)):
                raise _replaced(payload)
        else:
            _convert(manager, key, record, dest, columns)
        stale.unlink(missing_ok=True)
        manager.remove_older_releases(key, dest)
    return dest


def _table_path(payload: Path, token: str) -> Path:
    digest = hashlib.sha256(token.encode()).hexdigest()[:16]
    return payload.with_name(f"{payload.stem}-{digest}.parquet")


def _convert(
    manager: CacheManager,
    key: str,
    record: Mapping[str, typing.Any],
    dest: Path,
    columns: Mapping[str, BulkColumn | None],
) -> None:
    """Convert the cached zip of *record* into *dest*, atomically."""
    zip_path = manager.base_dir / record["filename"]
    # The open handle pins the bytes read. If the manifest still holds the
    # record the token came from, they are the bytes the token names.
    with zipfile.ZipFile(zip_path) as z:
        current = manager.find_record(key=key)
        if current is None or _payload_id(current[2]) != _payload_id(record):
            raise _replaced(zip_path)
        logger.info(f"Converting {zip_path.name} to Parquet as {dest.name}.")
        _atomic_write(dest, lambda tmp: _write_parquet(z, tmp, columns))

    # A clear() that ran during the conversion found no file to delete. One
    # that runs after this check finds dest and deletes it.
    if manager.find_record(key=key) is None:
        dest.unlink(missing_ok=True)
        raise _replaced(zip_path)


def _replaced(payload: Path) -> RuntimeError:
    return RuntimeError(
        f"{payload.name} was replaced or removed while scan() read it. Scan again."
    )


def _write_parquet(
    z: zipfile.ZipFile, dest: Path, columns: Mapping[str, BulkColumn | None]
) -> None:
    """Stream the zip's one CSV member into a Parquet file at *dest*."""
    members = [n for n in z.namelist() if n.endswith((".csv", ".txt"))]
    if len(members) != 1:
        raise ValueError(
            f"The bulk zip holds {len(members)} csv or txt files; expected 1."
        )
    with z.open(members[0]) as f:
        delimiter = _detect_delimiter(f)
        kept = converted_columns(_read_header(f, delimiter), columns)
        reader = pacsv.open_csv(
            f,
            # 4 MB blocks keep converting the 1.8 GB DAC2A csv near 0.5 GB of
            # memory; 16 MB blocks took it to 1.4 GB.
            read_options=pacsv.ReadOptions(block_size=1 << 22),
            parse_options=pacsv.ParseOptions(
                delimiter=delimiter, newlines_in_values=True
            ),
            convert_options=pacsv.ConvertOptions(
                column_types={raw: column.type for raw, column in kept.items()},
                include_columns=list(kept),
                strings_can_be_null=True,
            ),
        )
        schema = pa.schema(
            [pa.field(column.name, column.type) for column in kept.values()]
        )
        with pq.ParquetWriter(dest, schema, compression="zstd") as writer:
            pending = schema.empty_table()
            for batch in reader:
                batch_table = pa.Table.from_batches([batch]).rename_columns(
                    schema.names
                )
                pending = pa.concat_tables([pending, batch_table])
                full = pending.num_rows // _ROW_GROUP_ROWS * _ROW_GROUP_ROWS
                if full:
                    writer.write_table(
                        pending.slice(0, full), row_group_size=_ROW_GROUP_ROWS
                    )
                    pending = pending.slice(full)
            if pending.num_rows:
                writer.write_table(pending, row_group_size=_ROW_GROUP_ROWS)


def _read_header(f: typing.IO[bytes], delimiter: str) -> list[str]:
    """Read the header row of *f* and rewind it."""
    first_line = f.readline().decode("utf-8-sig")
    f.seek(0)
    reader = pacsv.read_csv(
        io.BytesIO(first_line.encode()),
        read_options=pacsv.ReadOptions(autogenerate_column_names=False),
        parse_options=pacsv.ParseOptions(delimiter=delimiter),
    )
    return reader.column_names
