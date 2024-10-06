import io
import re
import zipfile
from pathlib import Path

import pandas as pd
import requests

from oda_reader.common import api_response_to_df, logger
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

FALLBACK_STEP = 0.1
MAX_RETRIES = 5


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
    logger.info("Data downloaded correctly.")

    return df


def _save_or_return_parquet_files_from_content(
    response: requests.Response,
    save_to_path: Path | str | None = None,
) -> list[pd.DataFrame] | None:
    """Extracts parquet files from a zip archive in the response content.

    Args:
        response (requests.Response): The response object.
        save_to_path (Path | str | None): The path to save the file to. Optional. If
        not provided, a list of DataFrames is returned.

    Returns:
        list[pd.DataFrame]: The extracted DataFrames if save_to_path is not provided.
    """

    # Convert the save_to_path to a Path object
    save_to_path = Path(save_to_path) if save_to_path else None

    # Open the content as a zip file and extract the parquet files
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Find all parquet files in the zip archive
        parquet_files = [name for name in z.namelist() if name.endswith(".parquet")]

        # If save_to_path is provided, save the files to the path
        if save_to_path:
            save_to_path.mkdir(parents=True, exist_ok=True)
            for file_name in parquet_files:
                logger.info(f"Saving {file_name}")
                with z.open(file_name) as f_in, (save_to_path / file_name).open(
                    "wb"
                ) as f_out:
                    f_out.write(f_in.read())
            return

        # If save_to_path is not provided, return the DataFrames
        logger.info(f"Reading {len(parquet_files)} parquet files.")
        return [pd.read_parquet(z.open(file)) for file in parquet_files]


def _save_or_return_parquet_files_from_txt_in_zip(
    response: requests.Response,
    save_to_path: Path | str | None = None,
) -> list[pd.DataFrame] | None:
    """Extracts a .txt file from a zip archive in the response content, reads it as a CSV,
    and optionally saves it as a parquet file.

    Args:
        response (requests.Response): The response object.
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
    save_to_path = Path(save_to_path) if save_to_path else None

    # Open the content as a zip file and extract the parquet files
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
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


def bulk_download_parquet(
    file_id: str, save_to_path: Path | str | None = None, is_txt: bool = False
) -> pd.DataFrame | None:
    """Download data from the stats.oecd.org file download service.

    Certain data files are available as a bulk download in parquet format. This function
    downloads the parquet files and returns a single DataFrame.

    Args:
        file_id (str): The ID of the file to download.
        save_to_path (Path | str | None): The path to save the file to. Optional. If
        not provided, a DataFrame is returned.
        is_txt (bool): Whether the file is a .txt file. Defaults to False.

    Returns:
        pd.DataFrame | None: The DataFrame if save_to_path is not provided.
    """

    # Construct the URL
    file_url = BULK_DOWNLOAD_URL + file_id

    # Inform the user about what the function will do (save or return)
    if save_to_path:
        logger.info(f"Downloading parquet file and saving to {save_to_path}.")
    else:
        logger.info(
            "Downloading parquet file, which will be returned as a DataFrame. "
            "This may take a while..."
        )

    # Get the file
    response = requests.get(file_url, stream=True)
    # Check if the request was successful
    response.raise_for_status()

    # Read the parquet file
    if is_txt:
        files = _save_or_return_parquet_files_from_txt_in_zip(response, save_to_path)
    else:
        files = _save_or_return_parquet_files_from_content(response, save_to_path)

    if files:
        combined_df = pd.concat(files, ignore_index=True)
        logger.info("File downloaded correctly.")
        return combined_df

    logger.info("File saved correctly.")


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

    try:
        response = requests.get(f"{flow_url}{latest_flow}")
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        return get_bulk_file_id(
            flow_url=flow_url,
            search_string=search_string,
            latest_flow=round(latest_flow - FALLBACK_STEP, 1),
            retries=retries + 1,
        )

    content = response.text
    match = re.search(f"{re.escape(search_string)}(.*?)</", content)

    if not match:
        raise KeyError(f"The link to the bulk download file could not be found.")

    parquet_link = match.group(1).strip()

    return parquet_link.split("=")[-1]
