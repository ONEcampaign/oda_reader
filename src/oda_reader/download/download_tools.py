import csv
import hashlib
import io
import os
import re
import shutil
import tempfile
import typing
import warnings
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import requests

import oda_reader.download._deflate64  # noqa: F401  # adds Deflate64 support
from oda_reader._cache.dataframe import dataframe_cache
from oda_reader._cache.manager import CacheEntry, bulk_cache_manager
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
    validate_zip_or_raise,
)
from oda_reader.schemas.crs_translation import convert_crs_to_dotstat_codes
from oda_reader.schemas.dac1_translation import convert_dac1_to_dotstat_codes
from oda_reader.schemas.dac2_translation import convert_dac2a_to_dotstat_codes
from oda_reader.schemas.multisystem_translation import (
    convert_multisystem_to_dotstat_codes,
)
from oda_reader.schemas.schema_tools import (
    get_dtypes,
    preprocess,
    read_schema_translation,
)

BULK_DOWNLOAD_URL = "https://stats.oecd.org/wbos/fileview2.aspx?IDFile="
BASE_DATAFLOW = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/"
CRS_FLOW_URL = BASE_DATAFLOW + "DSD_CRS@DF_CRS/"
DAC2A_FLOW_URL = BASE_DATAFLOW + "DSD_DAC2@DF_DAC2A/"
MULTI_FLOW_URL = BASE_DATAFLOW + "DSD_MULTI@DF_MULTI/"
AIDDATA_VERSION = "3.0"
AIDDATA_DOWNLOAD_URL = (
    "https://docs.aiddata.org/ad4/datasets/"
    "AidDatas_Global_Chinese_Development_Finance_Dataset_Version_"
    f"{AIDDATA_VERSION.replace('.', '_')}.zip"
)


def _detect_delimiter(file_obj, sample_size: int = 8192) -> str:
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
            "filter_builder": qb.build_dac2a_filter,
            "convert_func": convert_dac2a_to_dotstat_codes,
        },
        "multisystem": {
            "filter_builder": qb.build_multisystem_filter,
            "convert_func": convert_multisystem_to_dotstat_codes,
        },
        "crs": {
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
    `DataFrame` objects or, when `as_iterator` is `True`, as an iterator
    yielding one `DataFrame` per row group (parquet only).

    The function auto-detects whether the zip contains parquet, csv, or txt files.
    CSV/txt files have their delimiter auto-detected (comma, pipe, tab, etc.) and
    are converted to parquet when saving.

    Args:
        response_content: Bytes or `Path` pointing to the zipped file.
        save_to_path: Optional path to save the files to.
        as_iterator: When `True` return an iterator that yields `DataFrame`
            objects for each row group. Defaults to ``False``. Only supported
            for parquet files.

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
                for file_name in parquet_files:
                    logger.info(f"Saving {file_name}")
                    dest_path = save_to_path / file_name
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    with (
                        z.open(file_name) as f_in,
                        dest_path.open("wb") as f_out,
                    ):
                        shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
                return None

            if as_iterator:
                # Return a generator over row groups
                return _iter_frames(response_content=response_content)

            logger.info(f"Reading {len(parquet_files)} parquet files.")
            return [pd.read_parquet(z.open(file)) for file in parquet_files]

        elif csv_files:
            if as_iterator:
                raise ValueError("Streaming not supported for csv/txt files.")

            if save_to_path:
                save_to_path.mkdir(parents=True, exist_ok=True)
                for file_name in csv_files:
                    clean_name = (
                        file_name.replace(".txt", ".parquet")
                        .replace(".csv", ".parquet")
                        .lower()
                        .replace(" ", "_")
                    )
                    logger.info(f"Saving {clean_name}")
                    with z.open(file_name) as f_in:
                        delimiter = _detect_delimiter(f_in)
                        logger.info(f"Detected delimiter: '{delimiter}'")
                        pd.read_csv(
                            f_in,
                            delimiter=delimiter,
                            encoding="utf-8",
                            quotechar='"',
                            low_memory=False,
                        ).to_parquet(save_to_path / clean_name)
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


def _stream_to_file(url: str, headers: dict, path: Path) -> None:
    """Stream a URL to the given file path."""

    logger.info(f"Streaming download from {url}")
    API_RATE_LIMITER.wait()
    with requests.get(url, headers=headers, stream=True) as r:
        if r.status_code > 299:
            raise BulkDownloadHTTPError(status_code=r.status_code, url=url, body=r.text)

        with path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


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


def _consume_bulk_zip(
    *,
    zip_path: Path,
    save_to_path: Path | str | None,
    as_iterator: bool,
    use_raw_cache: bool,
    manager: typing.Any,  # CacheManager | None — typed loosely to avoid an import cycle here
    url_key: str,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """Run extraction with cleanup tied to the no-cache temp file lifecycle.

    Three exit paths leak the temp file unless they're handled explicitly:
    BadZipFile, any other extraction error, and the lazy-iterator early
    return. This helper covers all three.
    """
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
        # Iterator construction is lazy. For the no-cache path, defer unlink
        # until the wrapping generator completes or is closed (CPython
        # guarantees close() on garbage collection).
        if not use_raw_cache:
            return _drain_then_unlink(files, zip_path)
        return files

    if not use_raw_cache:
        zip_path.unlink(missing_ok=True)

    if files:
        combined_df = pd.concat(files, ignore_index=True)
        logger.info("File downloaded / retrieved correctly.")
        return combined_df

    return None


def bulk_download_parquet(
    file_id: str,
    save_to_path: Path | str | None = None,
    is_txt: bool | None = None,
    *,
    as_iterator: bool = False,
    use_raw_cache: bool = True,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """Download data from the stats.oecd.org file download service.

    Certain data files are available as a bulk download. This function
    downloads the files (parquet, csv, or txt) and returns a single DataFrame.
    The file type is auto-detected from the zip contents.

    Args:
        file_id: The ID of the file to download.
        save_to_path: The path to save the file to. Optional.
            If not provided, the contents are returned.
        is_txt: Deprecated. File type is now auto-detected.
            This parameter is ignored and will be removed in a future version.
        as_iterator: When ``True`` return an iterator over ``DataFrame``
            chunks instead of a single ``DataFrame``. Useful for large files.
            Only supported for parquet files.
        use_raw_cache: If True (default), the raw zip is cached on disk and
            reused across calls. If False, the zip is downloaded to a
            temporary directory and deleted after extraction; each call hits
            the network. Integrity validation (is_zipfile + testzip) still
            runs in both modes.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    Raises:
        BulkPayloadCorruptError: If the downloaded zip fails integrity validation.
    """
    if is_txt is not None:
        warnings.warn(
            "The 'is_txt' parameter is deprecated and will be removed in a future "
            "version. File type (parquet or txt) is now auto-detected.",
            DeprecationWarning,
            stacklevel=2,
        )

    file_url = BULK_DOWNLOAD_URL + file_id
    headers = {"Accept-Encoding": "gzip"}

    if save_to_path:
        logger.info(f"The file will be saved to {save_to_path}.")
    else:
        logger.info("The file will be returned as a DataFrame.")

    url_key = hashlib.sha1(file_url.encode()).hexdigest()

    if use_raw_cache:
        entry = CacheEntry(
            key=url_key,
            filename=f"{url_key}.zip",
            fetcher=lambda p: _stream_to_file(file_url, headers, p),
        )
        manager = bulk_cache_manager()
        zip_path = manager.ensure(entry)
    else:
        manager = None
        zip_path = _stream_to_tempfile(file_url, headers)
        validate_zip_or_raise(zip_path)

    return _consume_bulk_zip(
        zip_path=zip_path,
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
        body = (
            response.decode("utf-8", errors="replace")
            if isinstance(response, bytes)
            else str(response)
        )
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
    """Retrieve the full bulk file ID from the OECD dataflow.

    The version to query is determined as follows:

    1. If *latest_flow* is provided, try that version first.
    2. If it fails (non-2xx or search string not found), call
       :func:`~oda_reader.download.version_discovery.discover_latest_version`
       to obtain the authoritative latest version and retry once.
    3. If *latest_flow* is ``None``, discover the version unconditionally
       before making any request.

    Args:
        flow_url: The base URL of the dataflow (without a version suffix),
            e.g. ``https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/``.
        search_string: The string to search for in the response XML to locate
            the bulk-download link.
        latest_flow: An explicit starting version to try.  If ``None`` the
            version is discovered automatically.

    Returns:
        str: The ID of the bulk download file.

    Raises:
        RuntimeError: If the bulk download file ID cannot be found even after
            version discovery.
    """
    headers = {"Accept-Encoding": "gzip"}

    dataflow_id = _extract_dataflow_id_from_flow_url(flow_url)

    def _try_version(version: float | int | str) -> str | None:
        """Attempt to retrieve the file ID for a given version.

        Returns the file ID on success, ``None`` if this version does not
        contain the expected link.
        """
        status, response, _ = _get_response_text(
            f"{flow_url}{version}", headers=headers
        )
        if status > 299:
            return None
        found = re.search(f"{re.escape(search_string)}(.*?)</", response)
        if not found:
            return None
        return found.group(1).strip().split("=")[-1]

    # --- Step 1: try an explicit version if provided ---
    if latest_flow is not None:
        result = _try_version(latest_flow)
        if result is not None:
            return result

    # --- Step 2 (or Step 1 when latest_flow is None): use version discovery ---
    if dataflow_id is not None:
        try:
            discovered_version = discover_latest_version(dataflow_id)
            result = _try_version(discovered_version)
            if result is not None:
                return result
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
            return result

    raise RuntimeError(
        f"Bulk download file ID for '{search_string}' could not be found "
        f"in dataflow '{flow_url}' after version discovery and scan."
    )
