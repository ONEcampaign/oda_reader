from typing import Literal
import pandas as pd
from pathlib import Path

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    bulk_download_aiddata,
)
from oda_reader.schemas.schema_tools import read_schema_translation, get_dtypes, preprocess

@cache_info
def download_aiddata(
    save_to_path: Path | str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    pre_process: bool = True,
) -> pd.DataFrame:
    """
    Download the AidData from the website.

    Args:
        save_to_path (Path | str): Path to save the raw data to.
        start_year (int): The start year of the data to return. This will filter based on commitment year. Optional
        end_year (int): The end year of the data to return. This will filter base on commitment year. Optional
        pre_process (bool): Whether to preprocess the data. Defaults to True.
    Returns:
        pd.DataFrame: The adiData data.

    """

    # Inform download is about to start
    logger.info(
        "Downloading AidData. This may take a while...\n"
    )

    df = bulk_download_aiddata(save_to_path=save_to_path)

    available_years = df["Commitment Year"].dropna().unique()
    years = df["Commitment Year"]

    # Build year filtering mask
    mask = pd.Series(True, index=df.index)

    if start_year is not None:
        if start_year in available_years:
            mask &= years >= start_year
        else:
            logger.debug(f"Ignoring start_year={start_year}; not in available years: {available_years.tolist()}")

    if end_year is not None:
        if end_year in available_years:
            mask &= years <= end_year
        else:
            logger.debug(f"Ignoring end_year={end_year}; not in available years: {available_years.tolist()}")

    df = df[mask]

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

if __name__ == "__main__":
    df = download_aiddata(start_year=2010, end_year=2030)