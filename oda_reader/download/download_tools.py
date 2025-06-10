import io
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
import hashlib

import pandas as pd
import requests
import typing
import pyarrow.parquet as pq

from oda_reader._cache import memory, cache_dir
from oda_reader.common import (
    api_response_to_df,
    logger,
    _cached_get_response_text,
    _get_response_text,
    _cached_get_response_content,
    API_RATE_LIMITER,
)
from oda_reader.download.query_builder import QueryBuilder
from oda_reader.schemas.crs_translation import convert_crs_to_dotstat_codes
from oda_reader.schemas.dac1_translation import convert_dac1_to_dotstat_codes
from oda_reader.schemas.dac2_translation import convert_dac2a_to_dotstat_codes
from oda_reader.schemas.multisystem_translation import (
    convert_multisystem_to_dotstat_codes,
)
from oda_reader.schemas.schema_tools import (
    read_schema_translation,
    get_dtypes,
    preprocess,
)

BULK_DOWNLOAD_URL = "https://stats.oecd.org/wbos/fileview2.aspx?IDFile="
BASE_DATAFLOW = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/"
CRS_FLOW_URL = BASE_DATAFLOW + "DSD_CRS@DF_CRS/"
MULTI_FLOW_URL = BASE_DATAFLOW + "DSD_MULTI@DF_MULTI/"
AIDDATA_VERSION = "3.0"
AIDDATA_DOWNLOAD_URL = (
    "https://docs.aiddata.org/ad4/datasets/"
    "AidDatas_Global_Chinese_Development_Finance_Dataset_Version_"
    f"{AIDDATA_VERSION.replace('.', '_')}.zip"
)

FALLBACK_STEP = 0.1
MAX_RETRIES = 5


def _open_zip(response_content: bytes | Path) -> zipfile.ZipFile:
    """Open a zip file from bytes or a file path."""
    if isinstance(response_content, (bytes, bytearray)):
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
    dataflow_version: str = None,
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

    # Get the dataframe

    df = api_response_to_df(url=url, read_csv_options=df_options)

    # Preprocess the data
    if pre_process:
        df = preprocess(df=df, schema_translation=schema_translation)
        if dotstat_codes:
            df = convert_func(df)
    else:
        if dotstat_codes:
            raise ValueError("Cannot convert to dotstat codes without preprocessing.")

    # Return the dataframe
    if not memory().store_backend:
        logger.info("Data downloaded correctly.")
    else:
        logger.info("Data loaded from cache correctly.")

    return df


def _save_or_return_parquet_files_from_content(
    response_content: bytes | Path,
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
) -> list[pd.DataFrame] | None | typing.Iterator[pd.DataFrame]:
    """Extract parquet files from a zip archive supplied as bytes or a file path.

    If `save_to_path` is provided the parquet files are extracted and written
    to disk. Otherwise the contents are returned either as a list of
    `DataFrame` objects or, when `as_iterator` is `True`, as an iterator
    yielding one `DataFrame` per row group. Iterating over row groups avoids
    materialising the entire file in memory at once.

    Args:
        response_content: Bytes or `Path` pointing to the zipped parquet file.
        save_to_path: Optional path to save the parquet files to.
        as_iterator: When `True` return an iterator that yields `DataFrame`
            objects for each row group. Defaults to ``False``.

    Returns:
        list[pd.DataFrame] | Iterator[pd.DataFrame] | None
    """

    save_to_path = Path(save_to_path).expanduser().resolve() if save_to_path else None

    with _open_zip(response_content=response_content) as z:
        parquet_files = [name for name in z.namelist() if name.endswith(".parquet")]

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


def _save_or_return_parquet_files_from_txt_in_zip(
    response_content: bytes | Path,
    save_to_path: Path | str | None = None,
) -> list[pd.DataFrame] | None:
    """Extract a `.txt` file from a zipped archive supplied as bytes or a file path.

    The file is read as CSV and optionally saved as a parquet file.

    Args:
        response_content: Bytes or ``Path`` pointing to the zipped archive.
        save_to_path (Path | str | None): The path to save the file to. Optional. If
        not provided, a list of DataFrames is returned.

    Returns:
        list[pd.DataFrame]: The extracted DataFrames if save_to_path is not provided.
    """
    oecd_txt_args = {
        "delimiter": "|",
        "encoding": "utf-8",
        "quotechar": '"',
        "low_memory": False,
    }

    # Convert the save_to_path to a Path object
    save_to_path = Path(save_to_path).expanduser().resolve() if save_to_path else None



    with _open_zip(response_content=response_content) as z:
        # Find all parquet files in the zip archive
        files = [name for name in z.namelist() if name.endswith(".txt")]

        # If save_to_path is provided, save the files to the path
        if save_to_path:
            save_to_path.mkdir(parents=True, exist_ok=True)
            for file_name in files:
                clean_name = (
                    file_name.replace(".txt", ".parquet").lower().replace(" ", "_")
                )
                logger.info(f"Saving {clean_name}")
                with z.open(file_name) as f_in:
                    pd.read_csv(f_in, **oecd_txt_args).to_parquet(
                        save_to_path / clean_name
                    )
            return

        # If save_to_path is not provided, return the DataFrames
        logger.info(f"Reading {len(files)} files.")
        return [pd.read_csv(z.open(file), **oecd_txt_args) for file in files]


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

        if save_to_path:
            save_to_path.mkdir(parents=True, exist_ok=True)
            output_file = save_to_path / Path(excel_file).name
            logger.info(f"Saving {excel_file} to {output_file}")
            with z.open(excel_file) as f_in, output_file.open("wb") as f_out:
                f_out.write(f_in.read())
            return None

        return pd.read_excel(z.open(excel_file), sheet_name=f"GCDF_{AIDDATA_VERSION}")


def _stream_to_file(url: str, headers: dict, path: Path) -> None:
    """Stream a URL to the given file path."""

    logger.info(f"Streaming download from {url}")
    API_RATE_LIMITER.wait()
    with requests.get(url, headers=headers, stream=True) as r:
        if r.status_code > 299:
            raise ConnectionError(f"Error {r.status_code}: {r.text}")

        with path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)


def _stream_to_tempfile(url: str, headers: dict) -> Path:
    """Download content to a temporary file using streaming."""

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        _stream_to_file(url, headers, Path(tmp.name))
        return Path(tmp.name)


def _cached_stream_to_file(url: str, headers: dict) -> Path:
    """Stream a URL to a cached file and return its path."""

    downloads = cache_dir()
    downloads.mkdir(parents=True, exist_ok=True)
    file_name = hashlib.sha1(url.encode()).hexdigest() + ".zip"
    destination = downloads / file_name
    if destination.exists():
        logger.info(f"Loading {url} from cache")
        return destination

    _stream_to_file(url, headers, destination)
    return destination

def _get_temp_file(file_url: str) -> tuple[Path, bool]:
    """Download file to a temporary location and return the path and a cleanup flag."""
    headers = {"Accept-Encoding": "gzip"}
    if memory().store_backend:
        temp_zip = _cached_stream_to_file(file_url, headers)
        cleanup = False
    else:
        temp_zip = _stream_to_tempfile(file_url, headers)
        cleanup = True
    return temp_zip, cleanup

def bulk_download_parquet(
    file_id: str,
    save_to_path: Path | str | None = None,
    is_txt: bool = False,
    *,
    as_iterator: bool = False,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """Download data from the stats.oecd.org file download service.

    Certain data files are available as a bulk download in parquet format. This function
    downloads the parquet files and returns a single DataFrame.

    Args:
        file_id (str): The ID of the file to download.
        save_to_path (Path | str | None): The path to save the file to. Optional.
            If not provided, the contents are returned.
        is_txt (bool): Whether the file is a .txt file. Defaults to False.
        as_iterator (bool): When ``True`` return an iterator over ``DataFrame``
            chunks instead of a single ``DataFrame``. Useful for large files.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None
    """
    # Construct the URL
    file_url = BULK_DOWNLOAD_URL + file_id

    # Inform the user about what the function will do (save or return)
    if save_to_path:
        logger.info(f"The file will be saved to {save_to_path}.")
    else:
        logger.info("The file will be returned as a DataFrame. ")

    # Download the zip file to avoid loading it fully in memory
    temp_zip_path, cleanup = _get_temp_file(file_url)

    try:
        # Read the parquet file
        if is_txt:
            if as_iterator:
                raise ValueError("Streaming not supported for txt files.")
            files = _save_or_return_parquet_files_from_txt_in_zip(
                response_content=temp_zip_path,
                save_to_path=save_to_path,
            )
        else:
            files = _save_or_return_parquet_files_from_content(
                response_content=temp_zip_path,
                save_to_path=save_to_path,
                as_iterator=as_iterator,
            )
            if as_iterator:
                return files
    except:
        if cleanup:
            os.unlink(temp_zip_path)
        raise Exception(
            f"Failed to read parquet files from {temp_zip_path}. "
            "Ensure the file is a valid zip archive containing parquet files."
        )

    if cleanup:
        os.unlink(temp_zip_path)

    if files:
        combined_df = pd.concat(files, ignore_index=True)
        logger.info("File downloaded / retrieved correctly.")
        return combined_df

    return None


@memory().cache
def _download_aiddata_response() -> bytes:
    logger.info("Downloading AidData. This may take a while...")
    headers = (("Accept-Encoding", "gzip"),)
    status, response = _cached_get_response_content(
        AIDDATA_DOWNLOAD_URL, headers=headers
    )
    if status > 299:
        raise ConnectionError(f"Error {status}: {response}")
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


def get_bulk_file_id(
    flow_url: str, search_string: str, latest_flow: float = 1.4, retries: int = 0
) -> str:
    """
    Retrieves the full bulk file ID from the OECD dataflow.

    Args:
        flow_url (str): The URL of the dataflow to check.
        search_string (str): The string to search for in the response content.
        latest_flow (float): The latest version of the dataflow to check.
        retries (int): The current number of retries (to avoid infinite recursion).

    Returns:
        str: The ID of the bulk download file.

    Raises:
        KeyError: If the bulk download file link could not be found.
        RuntimeError: If the maximum number of retries is exceeded.
    """
    if retries > MAX_RETRIES:
        raise RuntimeError(f"Maximum retries ({MAX_RETRIES}) exceeded.")

    if latest_flow == 1.0:
        latest_flow = int(round(latest_flow, 0))

    if memory().store_backend:
        get = _cached_get_response_text
    else:
        get = _get_response_text

    headers = (("Accept-Encoding", "gzip"),)
    status, response = get(f"{flow_url}{latest_flow}", headers=headers)

    if status > 299:
        return get_bulk_file_id(
            flow_url=flow_url,
            search_string=search_string,
            latest_flow=round(latest_flow - FALLBACK_STEP, 1),
            retries=retries + 1,
        )

    match = re.search(f"{re.escape(search_string)}(.*?)</", response)

    if not match:
        logger.info("The link to the bulk download file could not be found.")
        return get_bulk_file_id(
            flow_url=flow_url,
            search_string=search_string,
            latest_flow=round(latest_flow - FALLBACK_STEP, 1),
            retries=retries + 1,
        )

    parquet_link = match.group(1).strip()

    return parquet_link.split("=")[-1]
