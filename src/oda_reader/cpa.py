import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import download

DATAFLOW_ID: str = "DSD_CPA@DF_CRS_CPA"
DATAFLOW_VERSION: str = "1.4"

# CPA filter structure (dimension order mirrors CRS):
# donor, recipient, sector, measure, channel,
# modality, flow_type, price_base, md_dim, md_id, unit_measure,
# time_period


@cache_info
def download_cpa(
    start_year: int | None = None,
    end_year: int | None = None,
    filters: dict | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
    dataflow_version: str = DATAFLOW_VERSION,
) -> pd.DataFrame:
    """
    Download the CPA (Country Programmable Aid) data from the API.

    CPA is sourced directly from the OECD (`DSD_CPA@DF_CRS_CPA`), activity-level,
    and uses the same schema as CRS. Defaults to project-level microdata (`MD_DIM=DD`).

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        filters (dict): Optional filters to pass to the download.
        pre_process (bool): Whether to preprocess the data. Defaults to True. Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.
        dataflow_version (str): The version of the dataflow to download.

    Note:
        CPA has no grant-equivalent dataflow, so ``as_grant_equivalent`` is not
        available (unlike ``download_crs``).

    Returns:
        pd.DataFrame: The CPA data.

    """

    logger.info("Downloading CPA data. This may take a while — the OECD API is slow.")

    if filters is None:
        filters = {}

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
        version="cpa",
        dataflow_id=DATAFLOW_ID,
        dataflow_version=dataflow_version,
        start_year=start_year,
        end_year=end_year,
        filters=filters,
        pre_process=pre_process,
        dotstat_codes=dotstat_codes,
    )

    df = df.dropna(axis=1, how="all")

    return df
