import typing
import warnings
from pathlib import Path

import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    CRS_FLOW_URL,
    bulk_download_parquet,
    download,
    get_bulk_file_url,
    get_bulk_file_url_with_version,
)

DATAFLOW_ID: str = "DSD_CRS@DF_CRS"
DATAFLOW_ID_GE: str = "DSD_GREQ@DF_CRS_GREQ"
# Default version; actual version is discovered dynamically on fallback
DATAFLOW_VERSION: str = "1.6"

# CRS filter structure (dimension order):
# donor, recipient, sector, measure, channel,
# modality, flow_type, price_base, md_dim, md_id, unit_measure,
# time_period


def get_full_crs_parquet_url() -> str:
    return get_bulk_file_url(flow_url=CRS_FLOW_URL, label="CRS-Parquet")


def get_full_crs_parquet_id() -> str:
    """Deprecated alias for `get_full_crs_parquet_url`.

    Now returns a URL, not a file ID -- OECD annotations don't carry file
    IDs anymore. Kept only so existing two-step user code (an ID obtained
    here, then passed to `bulk_download_parquet`) keeps working.
    """
    warnings.warn(
        "get_full_crs_parquet_id is deprecated and now returns a URL, not a "
        "file ID. Use get_full_crs_parquet_url instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_full_crs_parquet_url()


def get_reduced_crs_parquet_url() -> str:
    return get_bulk_file_url(flow_url=CRS_FLOW_URL, label="CRS-reduced-parquet")


def get_reduced_crs_parquet_id() -> str:
    """Deprecated alias for `get_reduced_crs_parquet_url`; see its docstring."""
    warnings.warn(
        "get_reduced_crs_parquet_id is deprecated and now returns a URL, "
        "not a file ID. Use get_reduced_crs_parquet_url instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_reduced_crs_parquet_url()


def get_year_crs_zip_url(year: int | str) -> str:
    return get_bulk_file_url(
        flow_url=CRS_FLOW_URL, label=f"CRS {year} (dotStat format)"
    )


def get_year_crs_zip_id(year: int | str) -> str:
    """Deprecated alias for `get_year_crs_zip_url`; see its docstring."""
    warnings.warn(
        "get_year_crs_zip_id is deprecated and now returns a URL, not a "
        "file ID. Use get_year_crs_zip_url instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_year_crs_zip_url(year=year)


def download_crs_file(
    year: int | str,
    save_to_path: Path | str | None = None,
    *,
    as_iterator: bool = False,
    use_raw_cache: bool = True,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """
    Download a year of CRS data from the bulk download service. The file is large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        year: The year of CRS data to download.
        save_to_path: The path to save the file to. Optional. If not provided, a DataFrame is returned.
        as_iterator: If ``True`` yields ``DataFrame`` chunks instead of a single ``DataFrame``.
        use_raw_cache: If False, the raw zip is downloaded to a temporary directory and
            deleted after extraction. Each call hits the network. Validation still runs.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    """

    label = f"CRS {year} (dotStat format)"
    url, version = get_bulk_file_url_with_version(flow_url=CRS_FLOW_URL, label=label)

    return bulk_download_parquet(
        url=url,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
        use_raw_cache=use_raw_cache,
        flow_url=CRS_FLOW_URL,
        label=label,
        version=version,
    )


def bulk_download_crs(
    save_to_path: Path | str | None = None,
    reduced_version: bool = False,
    *,
    as_iterator: bool = False,
    use_raw_cache: bool = True,
) -> pd.DataFrame | None | typing.Iterator[pd.DataFrame]:
    """
    Bulk download the CRS data from the bulk download service. The file is very large.
    It is therefore strongly recommended to save it to disk. If save_to_path is not
    provided, the function will return a DataFrame.

    Args:
        save_to_path: The path to save the file to. Optional. If not provided, a DataFrame is returned.
        reduced_version: Whether to download the reduced version of the CRS data.
        as_iterator: If ``True`` yields ``DataFrame`` chunks instead of a single ``DataFrame``.
        use_raw_cache: If False, the raw zip is downloaded to a temporary directory and
            deleted after extraction. Each call hits the network. Validation still runs.

    Returns:
        pd.DataFrame | Iterator[pd.DataFrame] | None

    """

    label = "CRS-reduced-parquet" if reduced_version else "CRS-Parquet"
    url, version = get_bulk_file_url_with_version(flow_url=CRS_FLOW_URL, label=label)

    return bulk_download_parquet(
        url=url,
        save_to_path=save_to_path,
        as_iterator=as_iterator,
        use_raw_cache=use_raw_cache,
        flow_url=CRS_FLOW_URL,
        label=label,
        version=version,
    )


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
        pre_process (bool): Whether to preprocess the data. Defaults to True. Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        as_grant_equivalent (bool): Whether to download the grant equivalent data instead of flows.
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
