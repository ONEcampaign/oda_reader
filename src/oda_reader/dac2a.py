import typing
from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    DAC2A_FLOW_URL,
    bulk_download_parquet,
    download,
    get_bulk_file_id,
)

DATAFLOW_ID: str = "DSD_DAC2@DF_DAC2A"
DATAFLOW_VERSION: str = "1.6"


def get_full_dac2a_parquet_id() -> str:
    """Retrieve the file ID for the full DAC2A bulk download parquet file.

    Queries the OECD dataflow to find the bulk download link for the complete
    DAC2A dataset in dotStat format.

    Returns:
        str: The file ID to use with the bulk download service.

    Raises:
        RuntimeError: If the file ID cannot be found after maximum retries.
    """
    return get_bulk_file_id(
        flow_url=DAC2A_FLOW_URL, search_string="DAC2A full dataset (dotStat format)|"
    )


@cache_info
def download_dac2a(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    dataflow_version: str | None = None,
) -> pd.DataFrame:
    """
    Download the DAC2a data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True. Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        dataflow_version (str): The version of the data. Optional

    Returns:
        pd.DataFrame: The DAC2a data.

    """

    # Inform download is about to start
    logger.info("Downloading DAC2A data. This may take a while...")

    # Inform of the dataflow being downloaded
    if dataflow_version is None:
        dataflow_version = DATAFLOW_VERSION

    df = download(
        version="dac2a",
        dataflow_id=DATAFLOW_ID,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
        dataflow_version=dataflow_version,
    )

    return df


def bulk_download_dac2a(
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """
    Bulk download the DAC2a data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a DataFrame is returned.
        as_iterator: If ``True`` yields ``DataFrame`` chunks instead of a single ``DataFrame``.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    """
    file_id = get_full_dac2a_parquet_id()

    return bulk_download_parquet(
        file_id=file_id,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
    )
