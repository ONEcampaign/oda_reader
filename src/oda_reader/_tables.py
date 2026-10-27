"""Local Parquet tables for the DAC1, DAC2A, CRS and Multisystem bulk files.

`scan` serves each bulk file as one Parquet file on disk that a query engine
such as DuckDB reads in place. CRS is published as Parquet and is served from
the bulk cache unchanged. DAC1, DAC2A and Multisystem are published as zipped
CSVs, and are converted once per release into ``{key}-{digest}.parquet``
beside the cached zip, where ``digest`` hashes the release token.
"""

import contextlib
import hashlib
import io
import typing
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from filelock import FileLock

from oda_reader._cache.config import get_bulk_cache_dir
from oda_reader._cache.manager import ISO_FORMAT, CacheManager, bulk_cache_manager
from oda_reader.common import logger
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

type ScanDataset = typing.Literal["dac1", "dac2a", "crs", "multisystem"]

# The converted file's row-group size. DuckDB writes row groups of 122,880
# rows and skips any whose min/max statistics exclude a filter, so groups
# this size let a filter on year or donor skip most of a file.
_ROW_GROUP_ROWS = 122_880

# Columns per dataset for the zipped-CSV datasets. CRS is served as published.
_CONVERTED_COLUMNS: dict[str, Mapping[str, BulkColumn | None]] = {
    "dac1": DOTSTAT_TABLE_COLUMNS,
    "dac2a": DOTSTAT_TABLE_COLUMNS,
    "multisystem": MULTISYSTEM_COLUMNS,
}
_SCAN_DATASETS = ("dac1", "dac2a", "crs", "multisystem")


@dataclass(frozen=True)
class ScannedTable:
    """A bulk dataset as one local Parquet file.

    Attributes:
        path: The Parquet file. Read it in place; do not modify it.
        release: The release token of the bulk file it holds. See `release`.
        downloaded: When the bulk file was downloaded or registered (UTC).
        columns: The Parquet file's column names, in file order.
    """

    path: Path
    release: str
    downloaded: datetime
    columns: tuple[str, ...]


@dataclass(frozen=True)
class BulkFileInfo:
    """One file in the bulk file cache.

    Attributes:
        dataset: The `register_bulk_file` dataset name (``"crs"``,
            ``"dac1"`` and so on), or ``None`` for a file without one, such
            as a CRS year file.
        release: The file's release token, or ``None`` when none is recorded.
        downloaded: When the file was downloaded or registered (UTC).
        size_bytes: The file's size on disk, 0 if it is missing.
        stale: True when a downloaded file is past its time-to-live. An
            online read downloads it again; an offline read still uses it.
        path: The cached file.
    """

    dataset: str | None
    release: str | None
    downloaded: datetime
    size_bytes: int
    stale: bool
    path: Path


def scan(
    dataset: ScanDataset,
    *,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
    offline: bool = False,
) -> ScannedTable:
    """Return a bulk dataset as one local Parquet file, downloading it if needed.

    CRS is served from the bulk cache as published. DAC1, DAC2A and
    Multisystem are converted from their zipped CSV to Parquet the first time
    a release is scanned, and the converted file is reused until the release
    token changes. Converted columns are snake_case and unique
    case-insensitively (``DONOR`` and ``Donor`` become ``donor_code`` and
    ``donor_name``), so DuckDB reads them without renaming.

    Args:
        dataset: ``"dac1"``, ``"dac2a"``, ``"crs"`` or ``"multisystem"``.
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
        ScannedTable: The Parquet file, its release token, download time and
            column names.

    Raises:
        ValueError: If `dataset` is not one of the four names, or if both
            `refresh` and `offline` are True.
        BulkFileNotCachedError: If `offline` is True and the cache holds no
            copy.
        BulkDownloadChallengeError: If the file host challenges the download.
            Register a browser download with `register_bulk_file`.
        RuntimeError: If another process replaced the cached zip while it
            was being converted. Scanning again converts the new file.
    """
    _check_dataset(dataset)
    manager = _manager(cache_dir)
    key, path, record = _bulk_file(dataset, manager, refresh=refresh, offline=offline)
    token = _record_release(manager, key, path, record)

    columns = _CONVERTED_COLUMNS.get(dataset)
    table_path = (
        path if columns is None else _converted(manager, key, record, token, columns)
    )
    return ScannedTable(
        path=table_path,
        release=token,
        downloaded=_downloaded_at(record),
        columns=tuple(pq.read_schema(table_path).names),
    )


def release(
    dataset: ScanDataset,
    *,
    cache_dir: str | Path | None = None,
    refresh: bool = False,
    offline: bool = False,
) -> str:
    """Return the release token of a bulk dataset.

    Two copies of the same published file have the same token, whenever and
    wherever they were downloaded. For CRS the token is the readable release
    id OECD appends to the dataflow annotation, such as ``"v20260803"``. For
    DAC1, DAC2A and Multisystem it is the file host's ETag, else its
    Last-Modified date. A cached file with neither (a file registered while
    the host was unreachable) gets ``"sha256:<digest of the file>"``.

    Online, the token describes the file OECD publishes now, read with one
    request for the dataflow annotations and a one-byte request to the file
    host. Nothing is downloaded. When the host sends no token, the cached
    copy's token is returned.

    Args:
        dataset: ``"dac1"``, ``"dac2a"``, ``"crs"`` or ``"multisystem"``.
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
        url, label_release = _resolve_bulk_file(
            flow_url, label, bypass_annotation_cache=refresh
        )
        if dataset == "crs" and label_release is not None:
            return label_release
        if (version := _fetch_revalidation_token(url)) is not None:
            return version

    found = manager.find_record(flow_url=flow_url, label=label)
    if found is None:
        raise BulkFileNotCachedError(dataset=dataset, url=url)
    return _record_release(manager, *found)


def cache_info(*, cache_dir: str | Path | None = None) -> list[BulkFileInfo]:
    """List the files in the bulk file cache.

    Converted Parquet files are not listed. They sit beside the bulk file
    they were converted from and are deleted with it.

    Args:
        cache_dir: A cache root for this call only, as in `scan`.

    Returns:
        list[BulkFileInfo]: One entry per cached file, in manifest order.
    """
    manager = _manager(cache_dir)
    infos = []
    for row in manager.list_records():
        path = manager.base_dir / row["filename"]
        infos.append(
            BulkFileInfo(
                dataset=_bulk_dataset_name(row["flow_url"], row["label"]),
                release=row["release"] or row["version"],
                downloaded=_downloaded_at(row),
                size_bytes=path.stat().st_size if path.exists() else 0,
                stale=row["is_stale"],
                path=path,
            )
        )
    return infos


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
    if token := record.get("release") or record.get("version"):
        return token
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            digest.update(chunk)
    token = f"sha256:{digest.hexdigest()}"
    manager.set_release(key, token, seq=record.get("seq"))
    return token


def _downloaded_at(record: Mapping[str, typing.Any]) -> datetime:
    return datetime.strptime(record["downloaded_at"], ISO_FORMAT)


def _converted(
    manager: CacheManager,
    key: str,
    record: Mapping[str, typing.Any],
    token: str,
    columns: Mapping[str, BulkColumn | None],
) -> Path:
    """The Parquet conversion of the cached zip *key* for release *token*.

    The conversion is written once, under a lock so concurrent scans convert
    the file once, and removes earlier releases' conversions.

    Raises:
        RuntimeError: If another process replaced the cached zip after
            *record* was read, so *token* no longer names its contents.
    """
    zip_path = manager.base_dir / record["filename"]
    digest = hashlib.sha256(token.encode()).hexdigest()[:16]
    dest = zip_path.with_name(f"{zip_path.stem}-{digest}.parquet")
    if dest.exists():
        return dest

    with FileLock(str(zip_path.with_name(f".{zip_path.stem}.convert.lock"))):
        if dest.exists():
            return dest
        # The open handle pins the bytes read. If the manifest still holds the
        # record *token* came from, they are the bytes *token* names.
        with zipfile.ZipFile(zip_path) as z:
            current = manager.find_record(key=key)
            if current is None or current[2].get("seq") != record.get("seq"):
                raise RuntimeError(
                    f"{zip_path.name} was replaced while scan() read it. Scan again."
                )
            logger.info(f"Converting {zip_path.name} to Parquet for release {token}.")
            _atomic_write(dest, lambda tmp: _write_parquet(z, tmp, columns))

        for old in zip_path.parent.glob(f"{zip_path.stem}-*.parquet"):
            if old != dest:
                # A conversion another process holds open stays until next time.
                with contextlib.suppress(OSError):
                    old.unlink()
    return dest


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
