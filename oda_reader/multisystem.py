from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    get_bulk_file_id,
    MULTI_FLOW_URL,
    bulk_download_parquet,
    download,
)

DATAFLOW_ID: str = "DSD_MULTI@DF_MULTI"
DATAFLOW_VERSION: str = "1.6"


def get_full_multisystem_id():
    return get_bulk_file_id(
        flow_url=MULTI_FLOW_URL, search_string="Entire dataset (dotStat format)"
    )


def bulk_download_multisystem(save_to_path: Path | str | None = None):
    """
    Download the Multisystem data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a
        DataFrame is returned.


    Returns:
        pd.DataFrame | None: The DataFrame if save_to_path is not provided.

    """

    file_id = get_full_multisystem_id()

    return bulk_download_parquet(
        file_id=file_id, save_to_path=save_to_path, is_txt=True
    )


@cache_info
def download_multisystem(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    dataflow_version: str | None = None,
) -> pd.DataFrame:
    """
    Download Multisystem data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        dataflow_version (str): The version of the dataflow. Optional

    Returns:
        pd.DataFrame: The multisystem data.

    """

    # Inform download is about to start
    logger.info("Downloading Multisystem data. This may take a while...")

    # Inform of the dataflow being downloaded
    if dataflow_version is None:
        dataflow_version = DATAFLOW_VERSION
    logger.info(f"Downloading dataflow version {dataflow_version}")

    if not filters:
        filters = {}

    # Warn about duplicates
    if filters.get("microdata") is False:
        warning_message = "\nYou have requested aggregates.\n"
        warnings = [w for w in ("recipient", "sector") if w not in filters]

        if warnings:
            warning_message += "\n".join(
                f"Unless you specify {w}, the data will contain duplicates."
                for w in warnings
            )

        logger.warning(warning_message)

    df = download(
        version="multisystem",
        dataflow_id=DATAFLOW_ID,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
        dataflow_version=dataflow_version,
    )

    return df
