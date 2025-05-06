from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger

from oda_reader.download.download_tools import (
    bulk_download_aiddata,
)

@cache_info
def download_aiddata(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    as_grant_equivalent: bool = False,
) -> pd.DataFrame:
    """
    Download the AidData from the website.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        as_grant_equivalent (bool): Whether to download the grant equivalent data
        instead of flows.
    Returns:
        pd.DataFrame: The CRS data.

    """

    # Inform download is about to start
    logger.info(
        "Downloading AidData. This may take a while...\n"
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

    df = bulk_download_aiddata()
    
    # TODO: Clean data and standardize column and variable names 
    # for interoperability with OECD data.
    # Select years based on start_year and end_year
	# df = clean_raw_df(df)

    # download(
    #     version="crs",
    #     dataflow_id=DATAFLOW_ID if not as_grant_equivalent else DATAFLOW_ID_GE,
    #     dataflow_version=dataflow_version,
    #     start_year=start_year,
    #     end_year=end_year,
    #     filters=filters,
    #     pre_process=pre_process,
    #     dotstat_codes=dotstat_codes,
    # )

    # remove columns where all rows are NaN
    df = df.dropna(axis=1, how="all")

    return df