import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.download.download_tools import download

DATAFLOW_ID: str = "DSD_DAC1@DF_DAC1"
DATAFLOW_VERSION: str = "1.6"


@cache_info
def download_dac1(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    dataflow_version: str | None = None,
) -> pd.DataFrame:
    """
    Download the DAC1 data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        dataflow_version (str): The version of the dataflow. Optional

    Returns:
        pd.DataFrame: The DAC1 data.

    """

    # Inform of the dataflow being downloaded
    if dataflow_version is None:
        dataflow_version = DATAFLOW_VERSION

    df = download(
        version="dac1",
        dataflow_id=DATAFLOW_ID,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
        dataflow_version=dataflow_version,
    )

    return df
