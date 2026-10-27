import typing
from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.download.download_tools import (
    DAC1_FLOW_URL,
    bulk_download_parquet,
    download,
    get_bulk_file_url_with_version,
)

DATAFLOW_ID: str = "DSD_DAC1@DF_DAC1"

DAC1_BULK_LABEL = "DAC1 full dataset (dotStat format)"


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
        pre_process (bool): Whether to preprocess the data. Defaults to True. Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        dataflow_version (str | None): The dataflow version to read. Defaults to the latest published version.

    Returns:
        pd.DataFrame: The DAC1 data.

    """

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


def bulk_download_dac1(
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
    use_raw_cache: bool = True,
    offline: bool = False,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """
    Bulk download the DAC1 data from the bulk download service. The expanded file is
    205 MB, so save_to_path is recommended over holding it in memory. If save_to_path
    is not provided, the function returns a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a DataFrame is returned.
        as_iterator: If ``True``, yields ``DataFrame`` chunks one at a time. The source
            file is read into memory in full before it is sliced into chunks, so peak
            memory use matches the non-iterator path. This flag only bounds what the
            caller accumulates.
        use_raw_cache: If False, the raw zip is downloaded to a temporary directory and
            deleted after extraction. Each call hits the network. Validation still runs.
        offline: If True, make no network request and read the cached or
            registered copy (see `register_bulk_file`) whatever its age.
            Raises `BulkFileNotCachedError` when there is none.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    Note:
        Each call makes a one-byte ranged GET against the resolved URL to read
        the server's ETag (falling back to Last-Modified) and decide whether a
        cached copy is current. A failed lookup logs a warning and invalidation
        falls back to the 90-day TTL.

    """
    url, version = (
        (None, None)
        if offline
        else get_bulk_file_url_with_version(
            flow_url=DAC1_FLOW_URL, label=DAC1_BULK_LABEL
        )
    )

    return bulk_download_parquet(
        url=url,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
        use_raw_cache=use_raw_cache,
        flow_url=DAC1_FLOW_URL,
        label=DAC1_BULK_LABEL,
        version=version,
        offline=offline,
    )
