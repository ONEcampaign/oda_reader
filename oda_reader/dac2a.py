import pandas as pd

from oda_reader.common import logger
from oda_reader.download.download_tools import download

DATAFLOW_ID: str = "DSD_DAC2@DF_DAC2A"


def download_dac2a(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
) -> pd.DataFrame:
    """
    Download the DAC2a data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.

    Returns:
        pd.DataFrame: The DAC2a data.

    """

    # Inform download is about to start
    logger.info("Downloading DAC2A data. This may take a while...")

    df = download(
        version="dac2a",
        dataflow_id=DATAFLOW_ID,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
    )

    return df
