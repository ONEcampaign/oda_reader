import pandas as pd
from pathlib import Path

from oda_reader._cache import cache_info
from oda_reader.common import logger
from oda_reader.download.download_tools import (
    bulk_download_aiddata,
)
from oda_reader.schemas.schema_tools import read_schema_translation, convert_dtypes, preprocess


def filter_years(
        df,
        year_range: range | None = None,
) -> pd.DataFrame:
    """ Filters a dataframe by year range. If the provided time range is not a subset of the available years, it returns
    the entire dataframe.
    Args:
        df: the DataFrame to filter
        year_range: time range to filter by

    Returns:
        df: the filtered DataFrame
    """

    available_years = set(df["Commitment Year"].dropna().unique())

    if filter_years is not None and not set(year_range).issubset(available_years):
        logger.warning(
            f"Provided years %s are out of range. Will return all available years.",
            year_range,
        )
        return None

    return df.query("`Commitment Year` in @year_range")


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

    # Get data
    df = bulk_download_aiddata(save_to_path=save_to_path)

    # Filter years
    df = filter_years(df, range(start_year, end_year + 1))

    # get scheme for dtypes and column names
    schema = read_schema_translation(version="aidData")

    # Convert dtypes
    df = convert_dtypes(df, schema=schema)

    # rename/remove columns, convert bool columns
    if pre_process:
        df = preprocess(df, schema)

    # remove columns where all rows are NaN
    df = df.dropna(axis=1, how="all")

    return df