import csv
import hashlib
import io
import os
import re
import shutil
import tempfile
import time
import typing
import warnings
import zipfile
from html import unescape
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import requests

import oda_reader.download._deflate64  # noqa: F401  # adds Deflate64 support
from oda_reader._cache.config import _HOSTNAME
from oda_reader._cache.dataframe import dataframe_cache
from oda_reader._cache.manager import CacheEntry, bulk_cache_manager
from oda_reader._http_primitives import DEFAULT_HEADERS, _get_bulk_stream_session
from oda_reader.common import (
    API_RATE_LIMITER,
    _get_response_content,
    _get_response_text,
    api_response_to_df,
    logger,
)
from oda_reader.download.query_builder import QueryBuilder
from oda_reader.download.version_discovery import discover_latest_version
from oda_reader.exceptions import (
    BulkDownloadHTTPError,
    BulkPayloadCorruptError,
    validate_payload_or_raise,
)
from oda_reader.schemas.crs_translation import convert_crs_to_dotstat_codes
from oda_reader.schemas.dac1_translation import convert_dac1_to_dotstat_codes
from oda_reader.schemas.dac2_translation import convert_dac2_to_dotstat_codes
from oda_reader.schemas.multisystem_translation import (
    convert_multisystem_to_dotstat_codes,
)
from oda_reader.schemas.schema_tools import (
    get_dtypes,
    preprocess,
    read_schema_translation,
)

BASE_DATAFLOW = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/"
CRS_FLOW_URL = BASE_DATAFLOW + "DSD_CRS@DF_CRS/"
DAC1_FLOW_URL = BASE_DATAFLOW + "DSD_DAC1@DF_DAC1/"
DAC2A_FLOW_URL = BASE_DATAFLOW + "DSD_DAC2@DF_DAC2A/"
DAC2B_FLOW_URL = BASE_DATAFLOW + "DSD_DAC2@DF_DAC2B/"
MULTI_FLOW_URL = BASE_DATAFLOW + "DSD_MULTI@DF_MULTI/"
AIDDATA_VERSION = "3.0"
AIDDATA_DOWNLOAD_URL = (
    "https://docs.aiddata.org/ad4/datasets/"
    "AidDatas_Global_Chinese_Development_Finance_Dataset_Version_"
    f"{AIDDATA_VERSION.replace('.', '_')}.zip"
)

# Matches an EXT_RESOURCE annotation's English AnnotationText, whatever
# namespace prefix the server used -- the live XML uses `common:`, but the
# prefix is not part of the SDMX contract, so it's optional here rather than
# hardcoded.
_ANNOTATION_TEXT_RE = re.compile(
    r'<(?:\w+:)?AnnotationText[^>]*xml:lang="en"[^>]*>([^<]*)</(?:\w+:)?AnnotationText>'
)
# Strips the `-vYYYYMMDD` republish-date suffix OECD appends to labels, plus
# the optional `-N` tail a same-day republish adds.
_LABEL_VERSION_SUFFIX_RE = re.compile(r"-v\d{8}(?:-\d+)?$")

# The dataflow XML is a trusted OECD source, but it's remote input that
# becomes a request target once resolved -- this allowlist is what keeps a
# malformed or compromised annotation from turning into an SSRF vector. A
# CDN move is a one-line extension here.
_ALLOWED_URL_HOST_SUFFIXES = (".oecd.org",)

# `requests` follows redirects transparently by default, which would let a
# 302 from an allowlisted host carry the request off-allowlist without
# `_validate_allowed_host` ever seeing the new target. `_stream_to_file`
# follows redirects itself instead, capped here, re-validating every hop.
_MAX_REDIRECT_HOPS = 5

_PARQUET_MAGIC = b"PAR1"

# Rows per yielded chunk on the delimited (csv/txt) as_iterator path. Large
# enough that a single chunk is a meaningful, GC-friendly DataFrame; small
# enough that the annual CRS/CPA files (hundreds of thousands of rows) split
# into a handful of chunks rather than one, matching as_iterator's whole
# point of bounding memory.
_CSV_ITERATOR_CHUNK_SIZE = 100_000

# _stream_to_file retries a 403 (Cloudflare challenge) up to 3 times with
# this backoff. A challenge is normally deterministic per header set, so
# retries mostly buy latency -- but intermittent challenges were observed,
# so they do earn their keep. A 404 is never retried here; it fails fast
# into the caller's re-resolve path instead.
_STREAM_RETRY_BACKOFF_SECONDS = (1, 2, 4)

# Same set codelists/_fetch.py's _handshake_request retries on, and the same
# reasoning: these are genuinely transient transport failures (a timeout, a
# reset connection, a chunked response breaking mid-stream, a corrupt
# compressed body), not a verdict on the request itself. Catching
# requests.exceptions.RequestException wholesale would also retry permanent,
# unretryable failures -- MissingSchema/InvalidURL/InvalidSchema for a
# malformed URL (e.g. a pre-migration bare-GUID file_id passed through the
# deprecated kwarg) -- burning the full backoff on something a retry can
# never fix, and disguising a bad argument as a flaky network.
_TRANSIENT_STREAM_EXCEPTIONS = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
)

# Corruption signals for a lazy zip-member iterator (_iter_frames for
# parquet members, _iter_csv_chunks for delimited members). A member read
# can fail at the zip layer (bad CRC, discovered only once the member is
# actually read) on top of whatever its payload format raises once parsed.
_ZIP_ITERATOR_CORRUPT_EXCEPTIONS = (
    zipfile.BadZipFile,
    pyarrow.ArrowInvalid,
    pd.errors.ParserError,
    OSError,
)

# Corruption signals for the bare-parquet row-group iterator, matching
# _consume_bare_parquet's eager-read except clause.
_BARE_PARQUET_ITERATOR_CORRUPT_EXCEPTIONS = (pyarrow.ArrowInvalid, OSError)


def _validate_allowed_host(url: str) -> None:
    """Raise ValueError unless *url* is https with a host on the OECD allowlist.

    Applied independently at three points, so the check holds no matter how
    a URL entered the download path: once in `_finalize_resolved_url` for
    the freshly-resolved annotation URL, again inside `_stream_to_file` for
    whatever URL is actually about to be fetched (which covers a URL handed
    straight to `bulk_download_parquet(url=...)`, bypassing
    `get_bulk_file_url` entirely), and again on every redirect hop
    `_get_with_validated_redirects` follows (an allowlisted URL that
    server-side-redirects elsewhere would otherwise slip the check after the
    first hop, since `requests` follows redirects silently by default).

    Args:
        url: The URL to check.

    Raises:
        ValueError: If the scheme isn't ``https`` or the host doesn't end in
            one of `_ALLOWED_URL_HOST_SUFFIXES`.
    """
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    if parsed.scheme != "https" or not host.endswith(_ALLOWED_URL_HOST_SUFFIXES):
        raise ValueError(
            f"Refusing to fetch bulk-download URL with untrusted host {host!r}: {url}"
        )


def _detect_delimiter(file_obj: typing.IO[bytes], sample_size: int = 8192) -> str:
    """Detect the delimiter used in a CSV/text file.

    Reads a sample of the file and uses csv.Sniffer to detect the delimiter.
    Falls back to comma if detection fails.

    Args:
        file_obj: A file-like object to read from.
        sample_size: Number of bytes to sample for detection.

    Returns:
        str: The detected delimiter (typically ',' or '|').
    """
    sample = file_obj.read(sample_size)
    if isinstance(sample, bytes):
        sample = sample.decode("utf-8", errors="replace")

    # Reset file position
    file_obj.seek(0)

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
        return dialect.delimiter
    except csv.Error:
        # If sniffing fails, check which delimiter appears more often
        comma_count = sample.count(",")
        pipe_count = sample.count("|")
        return "|" if pipe_count > comma_count else ","


def _open_zip(response_content: bytes | Path) -> zipfile.ZipFile:
    """Open a zip file from bytes or a file path."""
    if isinstance(response_content, bytes | bytearray):
        return zipfile.ZipFile(io.BytesIO(response_content))
    return zipfile.ZipFile(response_content)


def _iter_frames(response_content: bytes | Path) -> typing.Iterator[pd.DataFrame]:
    """Iterate over row groups in parquet files within a zip archive."""
    with _open_zip(response_content) as z:
        parquet_files = [n for n in z.namelist() if n.endswith(".parquet")]
        for file_name in parquet_files:
            logger.info(f"Streaming {file_name}")
            with z.open(file_name) as f:
                pf = pq.ParquetFile(f)
                for rg in range(pf.num_row_groups):
                    yield pf.read_row_group(rg).to_pandas()


def _iter_csv_chunks(
    response_content: bytes | Path, chunk_size: int = _CSV_ITERATOR_CHUNK_SIZE
) -> typing.Iterator[pd.DataFrame]:
    """Iterate over chunks of delimited (csv/txt) files within a zip archive.

    The delimited analog of `_iter_frames`'s row-group iteration: the annual
    CRS and CPA bulk files are pipe-delimited `.txt` inside a zip rather
    than parquet, so `as_iterator` needs a chunked path for them too.

    Deliberately *not* `pd.read_csv(..., chunksize=...)`: that parses each
    chunk independently, and pandas' per-chunk dtype inference can disagree
    with a whole-file parse of the same data (observed on real CRS data --
    a code column with a leading zero read as string over the whole file,
    but silently coerced to a float and losing the leading zero when one
    particular chunk of the same column looked purely numeric in
    isolation). Reading the member whole with the exact same call the eager
    (non-iterator) path makes, then slicing the result, means this
    iterator's chunks are the same parse, just handed out in pieces --
    guaranteed identical to the non-iterator read of the same file, which a
    true streaming parse can't promise. `as_iterator` bounds what the
    *caller* accumulates, not the cost of this read; for the
    multi-hundred-thousand-row annual CRS/CPA files this targets, the
    non-iterator path already pays for one whole-file parse.

    Args:
        response_content: Bytes or `Path` pointing to the zipped file.
        chunk_size: Rows per yielded `DataFrame`.

    Yields:
        pd.DataFrame: One chunk per `chunk_size` rows, in member order.
    """
    with _open_zip(response_content) as z:
        csv_files = [
            name
            for name in z.namelist()
            if name.endswith(".txt") or name.endswith(".csv")
        ]
        for file_name in csv_files:
            logger.info(f"Streaming {file_name}")
            with z.open(file_name) as f_in:
                delimiter = _detect_delimiter(f_in)
                logger.info(f"Detected delimiter for {file_name}: '{delimiter}'")
                df = pd.read_csv(
                    f_in,
                    delimiter=delimiter,
                    encoding="utf-8",
                    quotechar='"',
                    low_memory=False,
                )
            for start in range(0, len(df), chunk_size):
                yield df.iloc[start : start + chunk_size].reset_index(drop=True)


def _safe_member_path(member_name: str, dest_dir: Path) -> Path | None:
    """Resolve a zip member name to a path inside `dest_dir`, or None if unsafe.

    Zip member names are attacker-controlled -- the archive could be a
    compromised OECD file or a redirect target that slipped the host
    allowlist. Joining a raw member name to a destination directory is a
    zip-slip / arbitrary-file-write: a `../../etc/cron.d/evil` entry
    traverses out of the destination, and an outright absolute entry like
    `/etc/cron.d/evil` is worse -- `Path(dest) / "/etc/cron.d/evil"`
    discards `dest` entirely, since pathlib evaluates the right operand's
    absoluteness first.

    Unlike flattening every member to its basename (this function's
    predecessor), this keeps the member's directory structure: two members
    with the same filename in different subdirectories, e.g.
    `a/data.parquet` and `b/data.parquet`, land at two different paths
    instead of the second silently overwriting the first. Every path
    *component* is checked instead -- an empty component, `.`, or `..`
    makes the whole member unsafe -- and the final resolved path is
    verified to still be inside `dest_dir` before ever being returned, as a
    defense-in-depth backstop against whatever the component check doesn't
    anticipate.

    Zip member names always use `/` as a separator regardless of platform
    (the zip spec requires it), so this parses with `PurePosixPath` rather
    than `Path` -- on Windows, `Path` would also treat a literal backslash
    in a malicious member name as a separator, which the zip format never
    intended.

    Args:
        member_name: A raw name from `ZipFile.namelist()`.
        dest_dir: The destination directory being extracted into.

    Returns:
        The resolved absolute path inside `dest_dir`, or `None` if the
        member name is unsafe (empty, absolute, contains a `.`/`..`
        component, or somehow still resolves outside `dest_dir`).
    """
    raw = PurePosixPath(member_name)
    if raw.is_absolute():
        return None

    parts = []
    for part in raw.parts:
        if not part or part in (".", ".."):
            return None
        parts.append(part)
    if not parts:
        return None

    dest_root = dest_dir.resolve()
    try:
        candidate = dest_root.joinpath(*parts).resolve()
    except (OSError, ValueError):
        return None
    if not candidate.is_relative_to(dest_root):
        return None
    return candidate


def _atomic_write(dest: Path, write_fn: typing.Callable[[Path], None]) -> None:
    """Write to `dest` atomically via a tmp-sibling then `Path.replace()`.

    The same pattern `CacheManager._fetch_and_cache` already uses for the
    raw cache download itself: `write_fn` writes the complete content to a
    host+pid-suffixed tmp sibling (so concurrent writers on a shared NFS
    mount can't collide), then a single `Path.replace()` swaps it into
    place. A disk-full or interrupted write leaves the tmp file truncated
    and `dest` untouched -- never a half-written file replacing a good one,
    and never a half-written file left masquerading as complete.

    Args:
        dest: Final destination path.
        write_fn: Callable that writes the complete content to the tmp path
            it is given (not `dest` itself).
    """
    tmp_path = Path(f"{dest}.tmp-{_HOSTNAME}-{os.getpid()}")
    try:
        write_fn(tmp_path)
        tmp_path.replace(dest)
    finally:
        tmp_path.unlink(missing_ok=True)


def download(
    version: str,
    dataflow_id: str,
    dataflow_version: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
) -> pd.DataFrame:
    """
    Download the data from the API.

    Args:
        version (str): The version of the data to download.
        dataflow_id (str): The dataflow id of the data to download.
        dataflow_version (str): The version of the dataflow. Optional
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.

    Returns:
        pd.DataFrame: The DAC1 data.

    """
    # Load the translation schema from .stat  to the new explorer
    schema_translation = read_schema_translation(version=version)

    # Get a data types dictionary
    data_types = get_dtypes(schema=schema_translation)

    # Set read csv options
    df_options = {
        "na_values": ("_Z", "nan"),
        "keep_default_na": True,
        "dtype": data_types,
    }

    # instantiate the query builder
    qb = QueryBuilder(dataflow_id=dataflow_id, dataflow_version=dataflow_version)

    # Define the version functions
    version_functions = {
        "dac1": {
            "filter_builder": qb.build_dac1_filter,
            "convert_func": convert_dac1_to_dotstat_codes,
        },
        "dac2a": {
            "filter_builder": qb.build_dac2_filter,
            "convert_func": convert_dac2_to_dotstat_codes,
        },
        "dac2b": {
            "filter_builder": qb.build_dac2_filter,
            "convert_func": convert_dac2_to_dotstat_codes,
        },
        "multisystem": {
            "filter_builder": qb.build_multisystem_filter,
            "convert_func": convert_multisystem_to_dotstat_codes,
        },
        "crs": {
            "filter_builder": qb.build_crs_filter,
            "convert_func": convert_crs_to_dotstat_codes,
        },
        "cpa": {
            "filter_builder": qb.build_crs_filter,
            "convert_func": convert_crs_to_dotstat_codes,
        },
    }

    try:
        filter_builder = version_functions[version]["filter_builder"]
        convert_func = version_functions[version]["convert_func"]
    except KeyError:
        raise ValueError(
            f"Version must be one of {', '.join(list(version_functions))}."
        )

    # Optionally set filters
    if isinstance(filters, dict):
        filter_str = filter_builder(**filters)
        qb.set_filter(filter_str)

    # Get the url
    url = qb.set_time_period(start=start_year, end=end_year).build_query()

    # Check DataFrame cache first (includes preprocessing params in key)
    df_cache = dataframe_cache()
    cached_df = df_cache.get(
        dataflow_id=dataflow_id,
        dataflow_version=dataflow_version or "default",
        url=url,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
    )

    if cached_df is not None:
        logger.info("Data loaded from DataFrame cache.")
        return cached_df

    # Cache miss - fetch from API (HTTP layer may still cache)
    df = api_response_to_df(url=url, read_csv_options=df_options)

    # Preprocess the data
    if pre_process:
        df = preprocess(df=df, schema_translation=schema_translation)
        if dotstat_codes:
            df = convert_func(df)
    elif dotstat_codes:
        raise ValueError("Cannot convert to dotstat codes without preprocessing.")

    # Cache the processed DataFrame
    df_cache.set(
        df=df,
        dataflow_id=dataflow_id,
        dataflow_version=dataflow_version or "default",
        url=url,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
    )

    logger.info("Data processed and cached.")
    return df


def _save_or_return_parquet_files_from_content(
    response_content: bytes | Path,
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
) -> list[pd.DataFrame] | None | typing.Iterator[pd.DataFrame]:
    """Extract parquet, csv, or txt files from a zip archive.

    If `save_to_path` is provided the files are extracted and written
    to disk. Otherwise the contents are returned either as a list of
    `DataFrame` objects or, when `as_iterator` is `True`, as an iterator --
    one `DataFrame` per row group for parquet, or one per
    `_CSV_ITERATOR_CHUNK_SIZE` rows for csv/txt.

    The function auto-detects whether the zip contains parquet, csv, or txt files.
    CSV/txt files have their delimiter auto-detected (comma, pipe, tab, etc.) and
    are converted to parquet when saving.

    Args:
        response_content: Bytes or `Path` pointing to the zipped file.
        save_to_path: Optional path to save the files to.
        as_iterator: When `True` return an iterator that yields `DataFrame`
            chunks instead of a single combined `DataFrame`. Defaults to
            ``False``.

    Returns:
        list[pd.DataFrame] | Iterator[pd.DataFrame] | None
    """

    save_to_path = Path(save_to_path).expanduser().resolve() if save_to_path else None

    with _open_zip(response_content=response_content) as z:
        parquet_files = [name for name in z.namelist() if name.endswith(".parquet")]
        csv_files = [
            name
            for name in z.namelist()
            if name.endswith(".txt") or name.endswith(".csv")
        ]

        # Determine which file type we're dealing with
        if parquet_files:
            if save_to_path:
                save_to_path.mkdir(parents=True, exist_ok=True)
                written_paths: set[str] = set()
                for file_name in parquet_files:
                    dest_path = _safe_member_path(file_name, save_to_path)
                    if dest_path is None:
                        logger.warning(
                            f"Skipping unsafe zip member name: {file_name!r}"
                        )
                        continue
                    # Path.__eq__ is case-sensitive regardless of the underlying
                    # filesystem, but APFS (macOS) and NTFS (Windows) are
                    # case-insensitive -- "A/data.parquet" and "a/data.parquet"
                    # are two different Paths that are the same file there.
                    # Casefold the *tracking key* only; dest_path itself, used
                    # for the actual write below, keeps its original case.
                    tracking_key = str(dest_path).casefold()
                    if tracking_key in written_paths:
                        raise ValueError(
                            f"Zip member {file_name!r} resolves to a destination "
                            f"path already used by another member: {dest_path}"
                        )
                    written_paths.add(tracking_key)
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Saving {dest_path.relative_to(save_to_path)}")

                    def _write_member(tmp_path: Path, *, _fn: str = file_name) -> None:
                        with z.open(_fn) as f_in, tmp_path.open("wb") as f_out:
                            shutil.copyfileobj(f_in, f_out, length=1024 * 1024)

                    _atomic_write(dest_path, _write_member)
                return None

            if as_iterator:
                # Return a generator over row groups
                return _iter_frames(response_content=response_content)

            logger.info(f"Reading {len(parquet_files)} parquet files.")
            return [pd.read_parquet(z.open(file)) for file in parquet_files]

        elif csv_files:
            if as_iterator:
                return _iter_csv_chunks(response_content=response_content)

            if save_to_path:
                save_to_path.mkdir(parents=True, exist_ok=True)
                written_paths = set()
                for file_name in csv_files:
                    dest_path = _safe_member_path(file_name, save_to_path)
                    if dest_path is None:
                        logger.warning(
                            f"Skipping unsafe zip member name: {file_name!r}"
                        )
                        continue
                    clean_name = (
                        dest_path.name.replace(".txt", ".parquet")
                        .replace(".csv", ".parquet")
                        .lower()
                        .replace(" ", "_")
                    )
                    dest_path = dest_path.with_name(clean_name)
                    tracking_key = str(dest_path).casefold()
                    if tracking_key in written_paths:
                        raise ValueError(
                            f"Zip member {file_name!r} resolves to a destination "
                            f"path already used by another member: {dest_path}"
                        )
                    written_paths.add(tracking_key)
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Saving {dest_path.relative_to(save_to_path)}")
                    with z.open(file_name) as f_in:
                        delimiter = _detect_delimiter(f_in)
                        logger.info(f"Detected delimiter: '{delimiter}'")
                        df = pd.read_csv(
                            f_in,
                            delimiter=delimiter,
                            encoding="utf-8",
                            quotechar='"',
                            low_memory=False,
                        )
                    _atomic_write(dest_path, lambda tmp, _df=df: _df.to_parquet(tmp))
                return None

            logger.info(f"Reading {len(csv_files)} csv/txt files.")
            dfs = []
            for file_name in csv_files:
                with z.open(file_name) as f_in:
                    delimiter = _detect_delimiter(f_in)
                    logger.info(f"Detected delimiter for {file_name}: '{delimiter}'")
                    dfs.append(
                        pd.read_csv(
                            f_in,
                            delimiter=delimiter,
                            encoding="utf-8",
                            quotechar='"',
                            low_memory=False,
                        )
                    )
            return dfs

        else:
            raise ValueError("No parquet, csv, or txt files found in the zip archive.")


def _save_or_return_bare_parquet(
    parquet_path: Path,
    url: str,
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
) -> list[pd.DataFrame] | None | typing.Iterator[pd.DataFrame]:
    """Extract a bare (non-zipped) parquet file's contents.

    Mirrors `_save_or_return_parquet_files_from_content`'s three modes, but
    for a payload that IS the parquet file rather than a zip containing one.
    There is exactly one "member" here, so it's named after the URL rather
    than a zip entry.

    Args:
        parquet_path: Path to the downloaded bare-parquet payload.
        url: The URL the payload was fetched from. Used only to name the
            file when `save_to_path` is given.
        save_to_path: Optional directory to copy the parquet file into,
            matching the zip path's directory semantics (`save_to_path` is
            always a directory in this API, never a target filename).
        as_iterator: When `True` return an iterator yielding one `DataFrame`
            per row group instead of a single `DataFrame`.

    Returns:
        list[pd.DataFrame] | Iterator[pd.DataFrame] | None
    """
    dest_name = Path(urlsplit(url).path).name or f"{parquet_path.name}.parquet"

    if save_to_path:
        dest_dir = Path(save_to_path).expanduser().resolve()
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving {dest_name}")

        def _copy_bare_parquet(tmp_path: Path) -> None:
            shutil.copyfile(parquet_path, tmp_path)

        _atomic_write(dest_dir / dest_name, _copy_bare_parquet)
        return None

    if as_iterator:
        logger.info(f"Streaming {dest_name}")
        pf = pq.ParquetFile(parquet_path)

        def _iter_row_groups() -> typing.Iterator[pd.DataFrame]:
            # try/finally, so that `pf` closes on the error path as well as
            # on normal exhaustion. Mid-iteration corruption is caught by
            # `_wrap_iterator_corruption`, which evicts and unlinks
            # `parquet_path`; leaving the handle open there races the
            # eviction, which POSIX tolerates but Windows refuses.
            try:
                for rg in range(pf.num_row_groups):
                    yield pf.read_row_group(rg).to_pandas()
            finally:
                pf.close()

        return _iter_row_groups()

    logger.info(f"Reading {dest_name}")
    return [pd.read_parquet(parquet_path)]


def _save_or_return_excel_files_from_content(
    response_content: bytes,
    save_to_path: Path | str | None = None,
) -> pd.DataFrame | None:
    """
    Extract exactly one Excel file from a zip archive in the response content.

    Args:
        response_content (bytes): Raw content from a requests.Response.
        save_to_path (Path | str | None): If provided, saves the file to this path.

    Returns:
        pd.DataFrame | None: The extracted DataFrame if not saving, else None.
    """
    save_to_path = Path(save_to_path).expanduser().resolve() if save_to_path else None

    with zipfile.ZipFile(io.BytesIO(response_content)) as z:
        excel_files = [
            info.filename
            for info in z.infolist()
            if info.filename.endswith(".xlsx")
            and not info.filename.startswith("__MACOSX/")
            and not info.filename.split("/")[-1].startswith("._")
            and not info.is_dir()
        ]

        if len(excel_files) != 1:
            raise ValueError(
                f"Expected exactly 1 Excel file, found {len(excel_files)}: {excel_files}"
            )

        excel_file = excel_files[0]
        df = pd.read_excel(z.open(excel_file), sheet_name=f"GCDF_{AIDDATA_VERSION}")

        if save_to_path:
            save_to_path.mkdir(parents=True, exist_ok=True)
            output_file = save_to_path / Path(excel_file).name
            logger.info(f"Saving {excel_file} as parquet to {output_file}")
            df = df.astype(
                {
                    "AidData Parent ID": "string[pyarrow]",
                    "Contact Position": "string[pyarrow]",
                }
            )
            df.to_parquet(output_file)
            return None

        return df


def _get_with_validated_redirects(
    session: requests.Session,
    url: str,
    headers: dict,
    *,
    timeout: tuple[int, int],
) -> requests.Response:
    """Stream-GET *url*, following redirects manually and re-validating each hop.

    ``requests`` follows redirects transparently by default -- so an
    allowlisted URL that server-side-redirects elsewhere would be followed
    without `_validate_allowed_host` ever seeing the new target, silently
    turning the allowlist into a first-hop-only check. Each hop here is
    validated *before* being followed; a hop that fails validation, or a
    chain that exceeds `_MAX_REDIRECT_HOPS`, raises rather than handing back
    a response to stream from.

    Args:
        session: The session to issue the request(s) through.
        url: The URL to fetch.
        headers: Headers to send with every hop.
        timeout: (connect, read) timeout passed to every hop.

    Returns:
        requests.Response: The terminal (non-redirect) response, still open
            for streaming.

    Raises:
        ValueError: If a hop's URL fails the host allowlist check.
        BulkDownloadHTTPError: If a redirect carries no ``Location`` header,
            or the chain exceeds `_MAX_REDIRECT_HOPS`.
    """
    for hop in range(_MAX_REDIRECT_HOPS + 1):
        _validate_allowed_host(url)
        response = session.get(
            url, headers=headers, stream=True, timeout=timeout, allow_redirects=False
        )
        if not (300 <= response.status_code < 400):
            return response

        location = response.headers.get("Location")
        response.close()
        if not location:
            raise BulkDownloadHTTPError(
                status_code=response.status_code,
                url=url,
                body="Redirect response carried no Location header.",
            )
        if hop == _MAX_REDIRECT_HOPS:
            raise BulkDownloadHTTPError(
                status_code=response.status_code,
                url=url,
                body=f"Exceeded {_MAX_REDIRECT_HOPS} redirect hops.",
            )
        url = urljoin(url, location)

    raise AssertionError("unreachable: loop above always returns or raises")


def _check_truncated(
    headers: typing.Mapping[str, str], bytes_written: int
) -> str | None:
    """Compare bytes actually written against the response's `Content-Length`.

    Truncation is the dominant real-world corruption mode for a large
    streamed download (a dropped connection mid-transfer), and this check
    is O(1) against headers already in hand -- far cheaper than deep-scanning
    the payload, and it catches the failure right where it happened instead
    of leaving it for `validate_payload_or_raise` to discover later.

    Skips the check (returns `None`) rather than false-positive when:
    - `Content-Length` is absent (some hosts don't send one), or
    - the response is `Content-Encoding`-compressed -- the header would be
      the *compressed* size, but `requests` transparently decompresses what
      `iter_content` hands back, so `bytes_written` counts decompressed
      bytes and the two are never comparable, or
    - `Content-Length` isn't a parseable integer.

    Args:
        headers: The response headers.
        bytes_written: Bytes actually written to disk this attempt.

    Returns:
        A human-readable description of the mismatch if truncated, else
        `None`.
    """
    if headers.get("Content-Encoding"):
        return None

    content_length = headers.get("Content-Length")
    if content_length is None:
        return None

    try:
        expected = int(content_length)
    except ValueError:
        return None

    if bytes_written != expected:
        return f"wrote {bytes_written} bytes, expected {expected} (Content-Length)"
    return None


def _stream_to_file(url: str, headers: dict, path: Path) -> None:
    """Stream a URL to the given file path.

    Uses the shared bulk-stream session with browser-like headers merged in
    (webfs-dcd.oecd.org 403s the package's plain headers behind a Cloudflare
    challenge) and a (10s connect, 60s read) timeout, since a hung
    connection would otherwise block forever.

    Every hop -- the initial URL and any redirect the server sends -- is
    checked against the OECD host allowlist by `_get_with_validated_redirects`.
    This also means the allowlist applies here regardless of how `url`
    arrived, including a URL handed straight to
    `bulk_download_parquet(url=...)` that never went through
    `get_bulk_file_url`.

    Retries a 403, a truncated transfer (see `_check_truncated` -- bytes
    written don't match `Content-Length`; truncation is the dominant
    real-world corruption mode for a large streamed download), or one of
    `_TRANSIENT_STREAM_EXCEPTIONS`, up to ``len(_STREAM_RETRY_BACKOFF_SECONDS)``
    times with that backoff. A 404 is never retried -- bulk-download URLs
    are permanently stable, so a 404 here means the URL was resolved from
    an annotation that's since gone stale in a way version-token
    invalidation didn't already catch (a retired dataflow version, or OECD
    renaming/removing the file), not a transient blip a retry could fix. It
    fails straight into the caller's re-resolve-and-retry-once path instead
    of burning backoff here. Nor is a permanent,
    request-is-malformed error like `MissingSchema` -- those can't be fixed
    by retrying, so they propagate on the first attempt (in practice
    `_validate_allowed_host` inside `_get_with_validated_redirects` already
    rejects a malformed URL before it reaches `requests` at all; this is the
    defense-in-depth backstop for whatever that check doesn't anticipate).
    Each attempt reopens ``path`` with "wb" (a partial write from a failed
    attempt is truncated, not appended to) and re-invokes
    ``API_RATE_LIMITER.wait()``, so retries stay inside the same
    process-wide throttle as every other request.
    """
    request_headers = {**DEFAULT_HEADERS, **headers}
    session = _get_bulk_stream_session()
    max_attempts = len(_STREAM_RETRY_BACKOFF_SECONDS) + 1

    for attempt, delay in enumerate((0, *_STREAM_RETRY_BACKOFF_SECONDS), start=1):
        if delay:
            time.sleep(delay)

        logger.info(f"Streaming download from {url} (attempt {attempt}/{max_attempts})")
        API_RATE_LIMITER.wait()
        is_last_attempt = attempt == max_attempts
        try:
            with _get_with_validated_redirects(
                session, url, request_headers, timeout=(10, 60)
            ) as r:
                if r.status_code > 299:
                    if r.status_code == 403 and not is_last_attempt:
                        logger.debug(
                            f"403 from {url} on attempt {attempt}/{max_attempts}; "
                            "retrying with a fresh connection."
                        )
                        continue
                    raise BulkDownloadHTTPError(
                        status_code=r.status_code, url=url, body=r.text
                    )

                bytes_written = 0
                with path.open("wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)

                truncation = _check_truncated(r.headers, bytes_written)
                if truncation is not None:
                    if not is_last_attempt:
                        logger.debug(
                            f"Truncated download from {url} on attempt "
                            f"{attempt}/{max_attempts}: {truncation}; retrying."
                        )
                        continue
                    raise BulkDownloadHTTPError(
                        status_code=r.status_code,
                        url=url,
                        body=f"Truncated download: {truncation}.",
                    )
                return
        except _TRANSIENT_STREAM_EXCEPTIONS:
            if is_last_attempt:
                raise
            logger.debug(
                f"Transport error streaming {url} on attempt {attempt}/"
                f"{max_attempts}; retrying."
            )
            continue


def _stream_to_tempfile(url: str, headers: dict) -> Path:
    """Download content to a temporary file using streaming.

    On stream failure the partial temp file is removed so callers don't have
    to track a partial download to clean up.
    """
    fd, name = tempfile.mkstemp()
    os.close(fd)
    path = Path(name)
    try:
        _stream_to_file(url, headers, path)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path


def _drain_then_unlink(
    iterable: typing.Iterable[pd.DataFrame], path: Path
) -> typing.Iterator[pd.DataFrame]:
    """Wrap an iterable so the source temp file is deleted on completion or close.

    Cleanup runs on normal exhaustion, on caller-side exceptions during
    iteration, and on generator close (CPython guarantees close() during
    garbage collection of the wrapping generator).
    """
    try:
        yield from iterable
    finally:
        path.unlink(missing_ok=True)


def _wrap_iterator_corruption(
    iterable: typing.Iterable[pd.DataFrame],
    *,
    path: Path,
    manager: typing.Any,  # CacheManager | None — typed loosely to avoid an import cycle here
    url_key: str,
    corrupt_exceptions: tuple[type[Exception], ...],
) -> typing.Iterator[pd.DataFrame]:
    """Wrap a lazy chunk iterator so mid-iteration corruption reaches eviction.

    `as_iterator=True` hands the caller a generator, not already-read data:
    constructing `_iter_frames`/`_iter_csv_chunks`/the bare-parquet
    row-group generator runs none of their body. So the try/except in
    `_consume_bulk_zip`/`_consume_bare_parquet` that wraps *constructing*
    that generator never sees a corruption that only surfaces once the
    caller actually starts pulling chunks -- by the time that happens, the
    try/except has long since returned. This re-homes the same conversion
    (raw exception -> `BulkPayloadCorruptError`, cache entry cleared) around
    the iteration itself, one shared implementation for every lazy path
    (zip-embedded parquet, zip-embedded delimited, bare parquet) rather than
    duplicating it per format.

    Args:
        iterable: The raw per-format generator (`_iter_frames`,
            `_iter_csv_chunks`, or the bare-parquet row-group generator).
        path: The cached or temp file backing `iterable`.
        manager: The `CacheManager` if `use_raw_cache=True`, else `None`.
        url_key: The manifest key to clear on corruption.
        corrupt_exceptions: Exception types that mean "the payload is
            corrupt," specific to the container/format this iterable reads.

    Yields:
        pd.DataFrame: Each chunk from `iterable`, unchanged.

    Raises:
        BulkPayloadCorruptError: If `iterable` raises one of
            `corrupt_exceptions` mid-iteration. The cache entry (or, for
            `use_raw_cache=False`, the file at `path`) is removed first.
    """
    try:
        yield from iterable
    except corrupt_exceptions as e:
        if manager is not None:
            manager.clear(url_key)
        else:
            path.unlink(missing_ok=True)
        raise BulkPayloadCorruptError(
            path, reason=f"{type(e).__name__} raised mid-iteration: {e}"
        ) from e


def _consume_bare_parquet(
    *,
    parquet_path: Path,
    url: str,
    save_to_path: Path | str | None,
    as_iterator: bool,
    use_raw_cache: bool,
    manager: typing.Any,  # CacheManager | None — typed loosely to avoid an import cycle here
    url_key: str,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """Bare-parquet analog of `_consume_bulk_zip`; same cleanup contract.

    `validate_payload_or_raise` already checked the footer before this runs,
    but footer-only validation is cheap specifically because it does *not*
    walk the file, so mid-file corruption (which a zip CRC walk would catch)
    surfaces here instead, at read time. The parquet analog of
    `zipfile.BadZipFile` is `pyarrow.ArrowInvalid`, with a plain `OSError`
    alongside it for a footer that points at a truncated file.
    """
    try:
        files = _save_or_return_bare_parquet(
            parquet_path=parquet_path,
            url=url,
            save_to_path=save_to_path,
            as_iterator=as_iterator,
        )
    except _BARE_PARQUET_ITERATOR_CORRUPT_EXCEPTIONS as e:
        # Reached by the eager (non-iterator) `pd.read_parquet` read, and by
        # a `pq.ParquetFile` construction failure in the as_iterator setup
        # above, before `_iter_row_groups`'s own try/finally is ever
        # entered. Neither leaves us an object to `.close()`: the open file
        # is referenced only via this except block's traceback for as long
        # as `e` is alive, which POSIX doesn't mind but Windows does.
        # Dropping the traceback releases it before we touch the file.
        e.__traceback__ = None
        if manager is not None:
            manager.clear(url_key)
        else:
            parquet_path.unlink(missing_ok=True)
        raise BulkPayloadCorruptError(
            parquet_path,
            reason=f"{type(e).__name__} raised when reading parquet: {e}",
        ) from e
    except BaseException as e:
        if not use_raw_cache:
            # Same traceback-retention hazard as above, for whatever
            # exception type falls through the corruption-specific branch.
            e.__traceback__ = None
            parquet_path.unlink(missing_ok=True)
        raise

    if as_iterator:
        if files is None:
            if not use_raw_cache:
                parquet_path.unlink(missing_ok=True)
            return None
        # `files` is a lazy generator here (row groups aren't read until
        # iterated), so corruption that the try/except above can't see --
        # it only runs synchronously at construction -- is caught here instead.
        wrapped = _wrap_iterator_corruption(
            files,
            path=parquet_path,
            manager=manager,
            url_key=url_key,
            corrupt_exceptions=_BARE_PARQUET_ITERATOR_CORRUPT_EXCEPTIONS,
        )
        if not use_raw_cache:
            return _drain_then_unlink(wrapped, parquet_path)
        return wrapped

    if not use_raw_cache:
        parquet_path.unlink(missing_ok=True)

    if files:
        combined_df = pd.concat(files, ignore_index=True)
        logger.info("File downloaded / retrieved correctly.")
        return combined_df

    return None


def _consume_bulk_zip(
    *,
    zip_path: Path,
    url: str,
    save_to_path: Path | str | None,
    as_iterator: bool,
    use_raw_cache: bool,
    manager: typing.Any,  # CacheManager | None — typed loosely to avoid an import cycle here
    url_key: str,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """Run extraction with cleanup tied to the no-cache temp file lifecycle.

    Dispatches on magic bytes first: `CRS.parquet` / `CRS-reduced.parquet`
    are served bare (`PAR1`), not zipped, unlike every other bulk file. The
    bare-parquet path is delegated to `_consume_bare_parquet`, which mirrors
    this function's cleanup contract exactly.

    For the zip path: three exit paths leak the temp file unless they're
    handled explicitly -- BadZipFile, any other extraction error, and the
    lazy-iterator early return. This helper covers all three.
    """
    with zip_path.open("rb") as f:
        magic = f.read(4)
    if magic == _PARQUET_MAGIC:
        return _consume_bare_parquet(
            parquet_path=zip_path,
            url=url,
            save_to_path=save_to_path,
            as_iterator=as_iterator,
            use_raw_cache=use_raw_cache,
            manager=manager,
            url_key=url_key,
        )

    try:
        files = _save_or_return_parquet_files_from_content(
            response_content=zip_path,
            save_to_path=save_to_path,
            as_iterator=as_iterator,
        )
    except zipfile.BadZipFile:
        if manager is not None:
            manager.clear(url_key)
        else:
            zip_path.unlink(missing_ok=True)
        raise BulkPayloadCorruptError(
            zip_path,
            reason="zipfile.BadZipFile raised when reading members",
        )
    except BaseException:
        if not use_raw_cache:
            zip_path.unlink(missing_ok=True)
        raise

    if as_iterator:
        if files is None:
            # save_to_path was provided; files were written to disk and there
            # is no iteration to wrap. Clean up immediately.
            if not use_raw_cache:
                zip_path.unlink(missing_ok=True)
            return None
        # Iterator construction is lazy -- a corrupt member (bad CRC, or
        # invalid parquet/delimited content) surfaces only once the caller
        # actually pulls chunks, which the try/except above can't see.
        wrapped = _wrap_iterator_corruption(
            files,
            path=zip_path,
            manager=manager,
            url_key=url_key,
            corrupt_exceptions=_ZIP_ITERATOR_CORRUPT_EXCEPTIONS,
        )
        # For the no-cache path, defer unlink until the wrapping generator
        # completes or is closed (CPython guarantees close() on garbage
        # collection).
        if not use_raw_cache:
            return _drain_then_unlink(wrapped, zip_path)
        return wrapped

    if not use_raw_cache:
        zip_path.unlink(missing_ok=True)

    if files:
        combined_df = pd.concat(files, ignore_index=True)
        logger.info("File downloaded / retrieved correctly.")
        return combined_df

    return None


def bulk_download_parquet(
    url: str | None = None,
    save_to_path: Path | str | None = None,
    is_txt: bool | None = None,
    *,
    file_id: str | None = None,
    flow_url: str | None = None,
    label: str | None = None,
    latest_flow: float | None = None,
    version: str | None = None,
    as_iterator: bool = False,
    use_raw_cache: bool = True,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """Download data from the OECD bulk file download service.

    Certain data files are available as a bulk download. This function
    downloads the file (a zip, or a bare parquet file for CRS.parquet /
    CRS-reduced.parquet) and returns a single DataFrame. The container
    format and, for zips, the member file type (parquet, csv, or txt) are
    both auto-detected.

    Args:
        url: The full URL of the file to download, as resolved by
            `get_bulk_file_url` or `get_bulk_file_url_with_version`.
        save_to_path: The path to save the file to. Optional.
            If not provided, the contents are returned.
        is_txt: Deprecated. File type is now auto-detected.
            This parameter is ignored and will be removed in a future version.
        file_id: Deprecated. OECD bulk-download annotations now carry full
            URLs rather than file IDs, so a value passed here is treated as
            a URL. Kept so existing two-step user code (an ID/URL obtained
            from one of the `get_full_*_id` helpers, then passed here) keeps
            working. Use `url` instead.
        flow_url: The dataflow URL the annotation was resolved from, e.g.
            `CRS_FLOW_URL`. Optional, but required (along with `label`) for
            the stale-annotation recovery below.
        label: The EXT_RESOURCE annotation label `url` was resolved from,
            e.g. `"CRS-Parquet"`. Optional, but required (along with
            `flow_url`) for the stale-annotation recovery below.
        latest_flow: An explicit starting dataflow version to pass through
            to `get_bulk_file_url_with_version` if a re-resolve is needed.
            Optional.
        version: The cache-invalidation token for `url`, as returned by
            `get_bulk_file_url_with_version` (the `-vYYYYMMDD` suffix from
            the annotation label, or an ETag/Last-Modified fallback for a
            label that carries no suffix). Threaded straight into
            `CacheEntry.version`, so a label republish forces a refetch even
            though the resolved URL itself is now permanently stable and
            the cache key (`sha1(url)`) alone can no longer tell a
            republish apart from a genuine cache hit. `None` (the default)
            means invalidation falls back to the cache's TTL alone -- the
            same behavior as before this parameter existed.
        as_iterator: When ``True`` return an iterator over ``DataFrame``
            chunks instead of a single ``DataFrame``. Useful for large files.
            One chunk per row group for parquet, or per
            `_CSV_ITERATOR_CHUNK_SIZE` rows for the delimited (csv/txt)
            format the annual CRS and CPA bulk files use.
        use_raw_cache: If True (default), the raw payload is cached on disk
            and reused across calls. If False, it is downloaded to a
            temporary directory and deleted after extraction; each call hits
            the network. Integrity validation still runs in both modes.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    Raises:
        BulkPayloadCorruptError: If the downloaded payload fails integrity
            validation.
        ValueError: If neither `url` nor the deprecated `file_id` is given,
            or if both are.

    Note:
        The annotation XML that `url` was resolved from is cached for 7
        days. Routine republishing is already handled elsewhere: the URL
        itself is permanently stable (unlike the old GUID URLs, which
        rotated on every republish), so `version` (see above) is what
        forces a refetch when OECD republishes -- before any request is
        even made. This recovery covers a narrower case: the resolved URL
        going dead between resolution and fetch, e.g. a retired dataflow
        version or a renamed/moved file. A plain retry would just
        re-request the same dead URL from the same stale annotation cache,
        so if `flow_url` and `label` are given, a final 404 or 403 here
        re-resolves the URL (and its version token) with that annotation
        cache bypassed and retries the download once before giving up.
    """
    if is_txt is not None:
        warnings.warn(
            "The 'is_txt' parameter is deprecated and will be removed in a future "
            "version. File type (parquet or txt) is now auto-detected.",
            DeprecationWarning,
            stacklevel=2,
        )

    if file_id is not None:
        warnings.warn(
            "The 'file_id' parameter is deprecated: bulk-download annotations "
            "now carry full URLs, not file IDs. The value is treated as a URL. "
            "Pass it as 'url' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if url is not None:
            raise ValueError("Pass either 'url' or the deprecated 'file_id', not both.")
        url = file_id

    if url is None:
        raise ValueError("bulk_download_parquet() requires a 'url' argument.")

    headers = {"Accept-Encoding": "gzip"}

    if save_to_path:
        logger.info(f"The file will be saved to {save_to_path}.")
    else:
        logger.info("The file will be returned as a DataFrame.")

    def _fetch(
        target_url: str, target_version: str | None
    ) -> tuple[typing.Any, Path, str]:
        """Resolve one URL to a validated local path, cached or not."""
        target_key = hashlib.sha1(target_url.encode()).hexdigest()
        extension = Path(urlsplit(target_url).path).suffix or ".zip"

        if use_raw_cache:
            entry = CacheEntry(
                key=target_key,
                filename=f"{target_key}{extension}",
                fetcher=lambda p: _stream_to_file(target_url, headers, p),
                version=target_version,
            )
            fetch_manager = bulk_cache_manager()
            fetched_path = fetch_manager.ensure(entry)
        else:
            fetch_manager = None
            fetched_path = _stream_to_tempfile(target_url, headers)
            validate_payload_or_raise(fetched_path)

        return fetch_manager, fetched_path, target_key

    try:
        manager, payload_path, url_key = _fetch(url, version)
    except BulkDownloadHTTPError as e:
        if e.status_code not in (404, 403) or flow_url is None or label is None:
            raise
        logger.info(
            f"{url} returned HTTP {e.status_code}; re-resolving with the "
            "annotation cache bypassed and retrying once."
        )
        url, version = get_bulk_file_url_with_version(
            flow_url, label, latest_flow, bypass_annotation_cache=True
        )
        manager, payload_path, url_key = _fetch(url, version)

    return _consume_bulk_zip(
        zip_path=payload_path,
        url=url,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
        use_raw_cache=use_raw_cache,
        manager=manager,
        url_key=url_key,
    )


def _download_aiddata_response() -> bytes:
    """Download AidData response (HTTP cached).

    Returns:
        bytes: The response content
    """
    logger.info("Downloading AidData. This may take a while...")
    headers = {"Accept-Encoding": "gzip"}
    status, response, _from_cache = _get_response_content(
        AIDDATA_DOWNLOAD_URL, headers=headers
    )
    if status > 299:
        # _get_response_content is typed to always return bytes for
        # `response`, so this never needs a str() fallback.
        body = response.decode("utf-8", errors="replace")
        raise BulkDownloadHTTPError(
            status_code=status, url=AIDDATA_DOWNLOAD_URL, body=body
        )
    return response


def bulk_download_aiddata(
    save_to_path: Path | str | None = None,
) -> pd.DataFrame | None:
    """
    Download data from the AidData website, extract the Excel file,
    and return as a DataFrame or save it to disk.

    Args:
        save_to_path (Path | str | None): The path to save the file to.

    Returns:
        pd.DataFrame | None: DataFrame if not saving, else None.
    """
    if save_to_path:
        logger.info(f"The file will be saved to {save_to_path}.")
    else:
        logger.info("The file will be returned as a DataFrame.")

    response = _download_aiddata_response()

    file = _save_or_return_excel_files_from_content(
        response_content=response,
        save_to_path=save_to_path,
    )

    if file is not None:
        logger.info("File downloaded / retrieved correctly.")
        return file

    return None


def _extract_dataflow_id_from_flow_url(flow_url: str) -> str | None:
    """Extract the dataflow ID from a bulk-download flow URL.

    Flow URLs follow the pattern::

        https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/<DATAFLOW_ID>/

    Args:
        flow_url: A URL string such as
            ``https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/``.

    Returns:
        The dataflow identifier (e.g. ``"DSD_CRS@DF_CRS"``) if found,
        ``None`` otherwise.
    """
    match = re.search(r"OECD\.DCD\.FSD/([^/]+)/?$", flow_url)
    return match.group(1) if match else None


def get_bulk_file_id(
    flow_url: str,
    search_string: str,
    latest_flow: float | None = None,
) -> str:
    """Deprecated: OECD bulk-download annotations no longer carry file IDs.

    The dataflow XML used to hold ``LABEL|<GUID>`` annotations; it now holds
    ``LABEL|<full URL>``. There is no ID left here to extract, so this
    always raises rather than returning something that looks plausible but
    can't be used to build a download URL.

    Args:
        flow_url: Unused; kept for signature compatibility.
        search_string: Unused; kept for signature compatibility.
        latest_flow: Unused; kept for signature compatibility.

    Raises:
        RuntimeError: Always. Points the caller at `get_bulk_file_url`.
    """
    # Intentionally unused: the signature is kept call-compatible so
    # existing positional/keyword callers hit this clear RuntimeError
    # instead of a TypeError.
    _ = (flow_url, search_string, latest_flow)
    raise RuntimeError(
        "get_bulk_file_id() no longer resolves anything usable: OECD "
        "bulk-download annotations now carry full URLs instead of file IDs. "
        "Use get_bulk_file_url(flow_url, label, latest_flow) instead."
    )


class _ExtResource(typing.NamedTuple):
    """One EXT_RESOURCE annotation: its URL and its `-vYYYYMMDD` version token.

    `version` is the matched suffix with the leading `-` dropped (e.g.
    ``"v20260803"`` or ``"v20260803-1"`` for a same-day republish), or
    `None` for a label that carries no suffix at all (DAC2A's doesn't).
    """

    url: str
    version: str | None


def _parse_ext_resources(xml: str) -> dict[str, _ExtResource]:
    """Map annotation label -> (URL, version) for every ``LABEL|URL`` annotation text.

    Scans every English (``xml:lang="en"``) ``AnnotationText`` in the
    dataflow XML for the ``LABEL|URL`` pattern OECD uses for bulk-download
    links, regardless of which ``AnnotationType`` it sits under. The French
    mirror alongside each one is ignored by construction (only ``en`` is
    matched), so it can never collide with or overwrite an English entry.

    Any ``-vYYYYMMDD`` (or same-day-republish ``-vYYYYMMDD-N``) suffix on
    the label is stripped before it becomes a dict key, so callers can look
    up a resource by its stable label regardless of when OECD last
    republished it -- the suffix itself is kept as `_ExtResource.version`
    rather than simply discarded, since it is exactly the signal a bulk
    file republish under an otherwise-permanently-stable URL needs to force
    a cache refetch (see `get_bulk_file_url_with_version`).

    Args:
        xml: Raw dataflow XML text.

    Returns:
        dict[str, _ExtResource]: Suffix-stripped label -> (URL, version).
    """
    resources: dict[str, _ExtResource] = {}
    for match in _ANNOTATION_TEXT_RE.finditer(xml):
        text = unescape(match.group(1))
        label, sep, url = text.partition("|")
        if not sep:
            continue
        label = label.strip()
        version_match = _LABEL_VERSION_SUFFIX_RE.search(label)
        version = (
            version_match.group(0)[1:] if version_match else None
        )  # drop leading "-"
        stripped_label = _LABEL_VERSION_SUFFIX_RE.sub("", label)
        resources[stripped_label] = _ExtResource(url=url.strip(), version=version)
    return resources


def _fetch_revalidation_token(url: str) -> str | None:
    """HEAD *url* for a lightweight cache-invalidation token (ETag, else Last-Modified).

    Used only when an annotation label carries no `-vYYYYMMDD` version
    suffix of its own (DAC2A's bulk label doesn't). Without this,
    `CacheEntry.version` would have nothing to compare against for that
    label, and a republish under the file's permanently-stable URL would be
    silently invisible to the cache until the TTL naturally expires --
    verified live against webfs-dcd.oecd.org: a HEAD with the same
    browser-like headers `_stream_to_file` uses returns 200 with both
    `ETag` and `Last-Modified` set.

    Best-effort: any transport error, non-2xx status, or a response
    carrying neither header falls back to `None` -- the same degraded
    TTL-only invalidation a versioned label gets if its annotation fetch
    fails outright. This never blocks the download; it only affects how
    eagerly a stale cache entry gets refetched.

    Subject to the same process-wide rate limiter as every other request
    this package makes (`_stream_to_file`, `_http_primitives`'s helpers) --
    this fires on every resolution of a label without a `-vYYYYMMDD` suffix
    against webfs-dcd.oecd.org, the one host already sensitive enough to
    need browser-like headers to clear its Cloudflare challenge, so it must
    not go out un-throttled.

    Args:
        url: The already-resolved, allowlist-checked bulk-download URL.

    Returns:
        The `ETag` header value, else `Last-Modified`, else `None`.
    """
    try:
        _validate_allowed_host(url)
        session = _get_bulk_stream_session()
        API_RATE_LIMITER.wait()
        response = session.head(
            url, headers=DEFAULT_HEADERS, timeout=(10, 30), allow_redirects=False
        )
    except (ValueError, requests.exceptions.RequestException) as e:
        logger.debug(f"Revalidation HEAD failed for {url}: {e}")
        return None

    if response.status_code > 299:
        return None
    return response.headers.get("ETag") or response.headers.get("Last-Modified")


def _finalize_resolved_url(url: str) -> str:
    """Percent-encode the path and enforce the OECD-host allowlist.

    Percent-encoding covers filenames with spaces (e.g. ``"CRS 2024
    data.zip"``); ``%`` stays in the safe set so an already-encoded URL is
    never double-encoded into ``%2520``.

    The dataflow XML this URL comes from is a trusted OECD source, but it's
    remote input that becomes a request target the moment it's resolved --
    the allowlist is what keeps a malformed or compromised annotation from
    becoming an SSRF vector.

    Args:
        url: The raw URL extracted from an annotation.

    Returns:
        str: The percent-encoded URL.

    Raises:
        ValueError: If the URL's scheme isn't ``https`` or its host doesn't
            end in one of `_ALLOWED_URL_HOST_SUFFIXES`.
    """
    parsed = urlsplit(url)
    encoded_path = quote(parsed.path, safe="/%")
    resolved = urlunsplit(
        (parsed.scheme, parsed.netloc, encoded_path, parsed.query, parsed.fragment)
    )
    _validate_allowed_host(resolved)
    return resolved


def get_bulk_file_url(
    flow_url: str,
    label: str,
    latest_flow: float | None = None,
    *,
    bypass_annotation_cache: bool = False,
) -> str:
    """Resolve the bulk-download URL for an EXT_RESOURCE annotation label.

    A thin wrapper over `get_bulk_file_url_with_version` that discards the
    version token, kept for backward compatibility with existing callers
    (including the documented two-step ``url = get_bulk_file_url(...)``
    pattern) that only want the URL. Callers that feed the result into
    `bulk_download_parquet` for caching should prefer
    `get_bulk_file_url_with_version` instead, so a republish under a
    stable URL actually invalidates the cache -- see that function's
    docstring.

    Args:
        flow_url: The base URL of the dataflow (without a version suffix),
            e.g. ``https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/``.
        label: The annotation label to look up, without its ``-vYYYYMMDD``
            suffix, e.g. ``"CRS-Parquet"``.
        latest_flow: An explicit starting version to try. If ``None`` the
            version is discovered automatically.
        bypass_annotation_cache: If ``True``, bypass the 7-day HTTP cache on
            the dataflow XML fetch and overwrite it with a fresh response.

    Returns:
        str: The resolved, percent-encoded, allowlist-checked URL.

    Raises:
        RuntimeError: If no URL for *label* can be found even after version
            discovery and scan.
        ValueError: If the resolved URL fails the host allowlist check.
    """
    url, _version = get_bulk_file_url_with_version(
        flow_url, label, latest_flow, bypass_annotation_cache=bypass_annotation_cache
    )
    return url


def get_bulk_file_url_with_version(
    flow_url: str,
    label: str,
    latest_flow: float | None = None,
    *,
    bypass_annotation_cache: bool = False,
) -> tuple[str, str | None]:
    """Resolve an EXT_RESOURCE annotation label to (url, cache-invalidation token).

    OECD dataflow XML carries a ``common:Annotation`` per bulk file, each
    with an English ``AnnotationText`` of the form ``LABEL|URL``. Labels
    gain a ``-vYYYYMMDD`` suffix that changes on every OECD republish;
    lookup here is an exact match against the label with that suffix
    stripped -- not a substring match, which is what previously let
    ``"CRS-Parquet"`` match inside ``"CRS-reduced-parquet"``.

    Because the URLs themselves are now permanently stable (``CRS.parquet``
    never changes, unlike the old GUID URLs that rotated on every
    republish), a bulk-file cache keyed on the URL alone can no longer tell
    a republish apart from a genuine cache hit -- the request wouldn't even
    be made. The returned token exists to close that gap when threaded into
    `CacheEntry.version` (see `bulk_download_parquet`'s `version` param):

    - If the label carries a ``-vYYYYMMDD`` suffix, the token is that
      suffix (without the leading ``-``).
    - If the label carries no suffix at all (DAC2A's bulk label doesn't),
      the token falls back to a `_fetch_revalidation_token` HEAD request
      for the resolved URL's `ETag`/`Last-Modified` -- a republish still
      changes *that*, even though the label's own text never rotates.
    - If neither is available (the HEAD also fails), the token is `None`
      and invalidation falls back to TTL alone, same as the degraded path
      for a versioned label whose annotation fetch fails outright.

    The version to query is determined as follows:

    1. If *latest_flow* is provided, try that version first.
    2. If it fails (non-2xx, or the label isn't found), call
       :func:`~oda_reader.download.version_discovery.discover_latest_version`
       to obtain the authoritative latest version and retry once.
    3. If *latest_flow* is ``None``, discover the version unconditionally
       before making any request.
    4. If discovery itself fails, fall back to a decrement scan from 2.0.

    Args:
        flow_url: The base URL of the dataflow (without a version suffix),
            e.g. ``https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/``.
        label: The annotation label to look up, without its ``-vYYYYMMDD``
            suffix, e.g. ``"CRS-Parquet"``.
        latest_flow: An explicit starting version to try. If ``None`` the
            version is discovered automatically.
        bypass_annotation_cache: If ``True``, bypass the 7-day HTTP cache on
            the dataflow XML fetch and overwrite it with a fresh response.
            Used by `bulk_download_parquet`'s stale-annotation recovery,
            for the narrower case that recovery covers (a retired dataflow
            version, or a renamed/moved file -- not routine republishing,
            which `version` already protects against before any request is
            made): the cached XML would otherwise keep serving a dead URL
            until it naturally expires.

    Returns:
        tuple[str, str | None]: The resolved, percent-encoded,
            allowlist-checked URL, and its cache-invalidation token (or
            `None` if none could be determined).

    Raises:
        RuntimeError: If no URL for *label* can be found even after version
            discovery and scan.
        ValueError: If the resolved URL fails the host allowlist check.
    """
    headers = {"Accept-Encoding": "gzip"}

    dataflow_id = _extract_dataflow_id_from_flow_url(flow_url)

    def _try_version(version: float | int | str) -> _ExtResource | None:
        """Attempt to resolve *label* to a URL for a given dataflow version."""
        status, response, _ = _get_response_text(
            f"{flow_url}{version}",
            headers=headers,
            force_refresh=bypass_annotation_cache,
        )
        if status > 299:
            return None
        return _parse_ext_resources(response).get(label)

    def _finalize(resource: _ExtResource) -> tuple[str, str | None]:
        resolved = _finalize_resolved_url(resource.url)
        version = resource.version
        if version is None:
            version = _fetch_revalidation_token(resolved)
        return resolved, version

    # --- Step 1: try an explicit version if provided ---
    if latest_flow is not None:
        result = _try_version(latest_flow)
        if result is not None:
            return _finalize(result)

    # --- Step 2 (or Step 1 when latest_flow is None): use version discovery ---
    if dataflow_id is not None:
        try:
            discovered_version = discover_latest_version(dataflow_id)
            result = _try_version(discovered_version)
            if result is not None:
                return _finalize(result)
        except (ConnectionError, ValueError):
            logger.info("Version discovery failed; falling back to version scan.")

    # --- Step 3: fall back to a decrement scan from 2.0 ---
    start = latest_flow if latest_flow is not None else 2.0
    for i in range(10):
        scan_version = round(start - i * 0.1, 1)
        if scan_version <= 0:
            break
        result = _try_version(scan_version)
        if result is not None:
            return _finalize(result)

    raise RuntimeError(
        f"Bulk download URL for label '{label}' could not be found "
        f"in dataflow '{flow_url}' after version discovery and scan."
    )
