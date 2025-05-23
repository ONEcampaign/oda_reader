from typing import Literal
import pandas as pd

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    bulk_download_aiddata,
)
from oda_reader.schemas.schema_tools import read_schema_translation, get_dtypes, preprocess


@cache_info
def download_aiddata(
    start_year: int | None = None,
    end_year: int | None = None,
    year_reference: Literal["commitment", "implementation", "completion"] = "commitment",
    pre_process: bool = True,
) -> pd.DataFrame:
    """
    Download the AidData from the website.

    Args:
        start_year (int): The start year of the data to return. Optional
        end_year (int): The end year of the data to return. Optional
        year_reference (Literal["commitment", "implementation", "completion"]): Whether to filter years based on
        commitment or implementation. Defaults to "commitment".
        pre_process (bool): Whether to preprocess the data. Defaults to True.
    Returns:
        pd.DataFrame: The adiData data.

    """

    # Inform download is about to start
    logger.info(
        "Downloading AidData. This may take a while...\n"
    )

    df = bulk_download_aiddata()

    year_column_map = {
        "commitment": "Commitment Year",
        "implementation": "Implementation Start Year",
        "completion": "Completion Year",
    }

    if start_year is not None:
        df = df.query(f"{year_column_map[year_reference]} >= {start_year}")

    if end_year is not None:
        df = df.query(f"{year_column_map[year_reference]} <= {end_year}")

    # get scheme for dtypes and column names
    schema = read_schema_translation(version="aidData")

    # convert dtypes
    dtypes = get_dtypes(schema=schema)
    for col in df.columns:
        dtype = dtypes[col]
        df[col] = df[col].astype(dtype)

    # rename/remove columns, convert bool columns
    if pre_process:
        df = preprocess(df, schema)

    # remove columns where all rows are NaN
    df = df.dropna(axis=1, how="all")

    return df