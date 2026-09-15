import typing
from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    DAC2B_FLOW_URL,
    bulk_download_parquet,
    download,
    get_bulk_file_url_with_version,
)

DATAFLOW_ID: str = "DSD_DAC2@DF_DAC2B"
# Default version; actual version is discovered dynamically on fallback
DATAFLOW_VERSION: str = "1.7"

DAC2B_BULK_LABEL = "DAC2B full dataset (dotStat format)"


@cache_info
def download_dac2b(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    dataflow_version: str | None = None,
) -> pd.DataFrame:
    """
    Download the DAC2b data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True. Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        dataflow_version (str): The version of the data. Optional

    Returns:
        pd.DataFrame: The DAC2b data.

    """

    # Inform download is about to start
    logger.info("Downloading DAC2B data. This may take a while...")

    # Inform of the dataflow being downloaded
    if dataflow_version is None:
        dataflow_version = DATAFLOW_VERSION

    df = download(
        version="dac2b",
        dataflow_id=DATAFLOW_ID,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
        dataflow_version=dataflow_version,
    )

    return df


def bulk_download_dac2b(
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
    use_raw_cache: bool = True,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """
    Bulk download the DAC2b data from the bulk download service. The expanded file is
    459.8 MB and 2,281,210 rows (20.6 MB compressed). Either way, the conversion reads
    the full CSV into memory before writing parquet, so save_to_path reduces what you
    retain afterward, not the peak memory the conversion itself uses. If save_to_path
    is not provided, the function returns a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a DataFrame is returned.
        as_iterator: If ``True`` yields ``DataFrame`` chunks instead of a single ``DataFrame``.
        use_raw_cache: If False, the raw zip is downloaded to a temporary directory and
            deleted after extraction. Each call hits the network. Validation still runs.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    Note:
        OECD stamps no publication version on the DAC2B annotation label, so each
        call makes a one-byte ranged GET against the resolved URL to read the
        server's ETag (falling back to Last-Modified) and decide whether a cached
        copy is current. A failed lookup logs a warning and invalidation falls
        back to the 30-day TTL.

    """
    # DAC2B's label carries no -vYYYYMMDD suffix (see get_bulk_file_url_with_version),
    # so `version` here falls back to an ETag/Last-Modified ranged-GET token.
    # That token is enough to force a refetch on republish.
    url, version = get_bulk_file_url_with_version(
        flow_url=DAC2B_FLOW_URL, label=DAC2B_BULK_LABEL
    )

    return bulk_download_parquet(
        url=url,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
        use_raw_cache=use_raw_cache,
        flow_url=DAC2B_FLOW_URL,
        label=DAC2B_BULK_LABEL,
        version=version,
    )
