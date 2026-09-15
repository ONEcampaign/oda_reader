import typing
import warnings
from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    DAC2A_FLOW_URL,
    bulk_download_parquet,
    download,
    get_bulk_file_url,
    get_bulk_file_url_with_version,
)

DATAFLOW_ID: str = "DSD_DAC2@DF_DAC2A"
# Default version; actual version is discovered dynamically on fallback
DATAFLOW_VERSION: str = "1.4"

DAC2A_BULK_LABEL = "DAC2A full dataset (dotStat format)"


def get_full_dac2a_parquet_url() -> str:
    """Retrieve the bulk download URL for the full DAC2A parquet dataset.

    Queries the OECD dataflow to find the bulk download link for the complete
    DAC2A dataset in dotStat format.

    Returns:
        str: The URL to use with the bulk download service.

    Raises:
        RuntimeError: If the URL cannot be found after maximum retries.
    """
    return get_bulk_file_url(flow_url=DAC2A_FLOW_URL, label=DAC2A_BULK_LABEL)


def get_full_dac2a_parquet_id() -> str:
    """Deprecated alias for `get_full_dac2a_parquet_url`.

    Now returns a URL, not a file ID -- OECD annotations don't carry file
    IDs anymore. Kept only so existing two-step user code (an ID obtained
    here, then passed to `bulk_download_parquet`) keeps working.
    """
    warnings.warn(
        "get_full_dac2a_parquet_id is deprecated and now returns a URL, not "
        "a file ID. Use get_full_dac2a_parquet_url instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_full_dac2a_parquet_url()


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
    use_raw_cache: bool = True,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """
    Bulk download the DAC2a data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a DataFrame is returned.
        as_iterator: If ``True`` yields ``DataFrame`` chunks instead of a single ``DataFrame``.
        use_raw_cache: If False, the raw zip is downloaded to a temporary directory and
            deleted after extraction. Each call hits the network. Validation still runs.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    """
    # DAC2A's label carries no -vYYYYMMDD suffix (see get_bulk_file_url_with_version),
    # so `version` here falls back to an ETag/Last-Modified ranged-GET token.
    # That token is enough to force a refetch on republish.
    url, version = get_bulk_file_url_with_version(
        flow_url=DAC2A_FLOW_URL, label=DAC2A_BULK_LABEL
    )

    return bulk_download_parquet(
        url=url,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
        use_raw_cache=use_raw_cache,
        flow_url=DAC2A_FLOW_URL,
        label=DAC2A_BULK_LABEL,
        version=version,
    )
