import io
import zipfile

import pandas as pd
import requests

from oda_reader.common import api_response_to_df, logger
from oda_reader.download.query_builder import QueryBuilder
from oda_reader.schemas.dac1_translation import convert_dac1_to_dotstat_codes
from oda_reader.schemas.dac2_translation import convert_dac2a_to_dotstat_codes
from oda_reader.schemas.schema_tools import (
    read_schema_translation,
    get_dtypes,
    preprocess,
)

BULK_DOWNLOAD_URL = "https://stats.oecd.org/wbos/fileview2.aspx?IDFile="


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

    # Select right filter builder and dotstat codes
    if version == "dac1":
        filter_builder = qb.build_dac1_filter
        convert_func = convert_dac1_to_dotstat_codes
    elif version == "dac2a":
        filter_builder = qb.build_dac2a_filter
        convert_func = convert_dac2a_to_dotstat_codes
    else:
        raise ValueError("Version must be either 'dac1' or 'dac2a'.")

    # Optionally set filters
    if filters:
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


def _extract_parquet_files_from_content(
    response: requests.Response,
) -> list[pd.DataFrame]:
    # Open the content as a zip file and extract the parquet files
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # Find all parquet files in the zip archive
        parquet_filenames = [name for name in z.namelist() if name.endswith(".parquet")]

        # List to store the DataFrames
        files = []

        # Loop over the parquet files
        for file in parquet_filenames:
            # Extract and load the Parquet file into a DataFrame
            logger.info(f"Extracting and loading {file}")
            with z.open(file) as f:
                df = pd.read_parquet(f)
                files.append(df)

    return files


def bulk_download_parquet(file_id: str) -> pd.DataFrame:
    """Download data from the stats.oecd.org file download service.

    Certain data files are available as a bulk download in parquet format. This function
    downloads the parquet files and returns a single DataFrame.

    Args:
        file_id (str): The ID of the file to download.

    Returns:
        pd.DataFrame: The data.
    """

    # Construct the URL
    file_url = BULK_DOWNLOAD_URL + file_id

    # Inform download is about to start
    logger.info("Downloading parquet file. This may take a while...")

    # Get the file
    response = requests.get(file_url)

    # Check if the request was successful
    response.raise_for_status()

    # Read the parquet file
    files = _extract_parquet_files_from_content(response)

    df = pd.concat(files, ignore_index=True)

    # Inform download is complete
    logger.info("Parquet file downloaded correctly.")

    return df
