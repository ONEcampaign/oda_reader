from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    get_bulk_file_id,
    bulk_download_parquet,
    CRS_FLOW_URL,
    download,
)

DATAFLOW_ID: str = "DSD_CRS@DF_CRS"
DATAFLOW_ID_GE: str = "DSD_GREQ@DF_CRS_GREQ"
DATAFLOW_VERSION: str = "1.6"

"""
{donor}.{recipient}.{sector}.{measure}.{channel}.
        {modality}.{flow_type}.{price_base}.{md_dim}.{md_id}.{unit_measure}.
        {time_period}
"""


def get_full_crs_parquet_id():
    return get_bulk_file_id(flow_url=CRS_FLOW_URL, search_string="CRS-Parquet")


def get_reduced_crs_parquet_id():
    return get_bulk_file_id(flow_url=CRS_FLOW_URL, search_string="CRS-reduced-parquet")


def get_year_crs_zip_id(year: int):
    return get_bulk_file_id(
        flow_url=CRS_FLOW_URL, search_string=f"CRS {year} (dotStat format)"
    )


def download_crs_file(year: int | str, save_to_path: Path | str | None = None):
    """
    Download a year of CRS data from the bulk download service. The file is large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        year: The year of CRS data to download.
        save_to_path: The path to save the file to. Optional. If not provided, a
        DataFrame is returned.

    Returns:
        pd.DataFrame | None: The DataFrame if save_to_path is not provided.

    """

    file_id = get_year_crs_zip_id(year=year)

    return bulk_download_parquet(
        file_id=file_id, save_to_path=save_to_path, is_txt=True
    )


def bulk_download_crs(
    save_to_path: Path | str | None = None, reduced_version: bool = False
):
    """
    Bulk download the CRS data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a
        DataFrame is returned.
        reduced_version: Whether to download the reduced version of the CRS data.

    Returns:
        pd.DataFrame | None: The DataFrame if save_to_path is not provided.

    """

    if reduced_version:
        file_id = get_reduced_crs_parquet_id()
    else:
        file_id = get_full_crs_parquet_id()

    return bulk_download_parquet(file_id=file_id, save_to_path=save_to_path)


@cache_info
def download_crs(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    as_grant_equivalent: bool = False,
    dataflow_version: str = DATAFLOW_VERSION,
) -> pd.DataFrame:
    """
    Download the CRS data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        as_grant_equivalent (bool): Whether to download the grant equivalent data
        instead of flows.
        dataflow_version (str): The version of the dataflow to download.

    Returns:
        pd.DataFrame: The CRS data.

    """

    # Inform download is about to start
    logger.info(
        "Downloading CRS data. This may take a while...\n"
        "Note this is a slow API. Consider using bulk_download_crs() to download"
        "the full dataset instead."
    )

    if filters is None:
        filters = {}

    # Warn about duplicates
    if filters.get("microdata") is False:
        warning_message = "\nYou have requested aggregates.\n"
        warnings = [w for w in ("channel", "modality") if w not in filters]

        if warnings:
            warning_message += "\n".join(
                f"Unless you specify {w}: '_T', the data will contain duplicates."
                for w in warnings
            )

        logger.warning(warning_message)

    df = download(
        version="crs",
        dataflow_id=DATAFLOW_ID if not as_grant_equivalent else DATAFLOW_ID_GE,
        dataflow_version=dataflow_version,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
    )

    # remove columns where all rows are NaN
    df = df.dropna(axis=1, how="all")

    return df
