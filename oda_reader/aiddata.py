import pandas as pd
from pathlib import Path

from oda_reader._cache import cache_info
from oda_reader.download.download_tools import (
    bulk_download_aiddata,
)
from oda_reader.schemas.schema_tools import (
    read_schema_translation,
    convert_dtypes,
    preprocess,
)


def filter_years(
    df: pd.DataFrame,
    start_year: int = None,
    end_year: int = None,
) -> pd.DataFrame:
    """Filters a dataframe by year range. If the provided time range is not a subset of the available years, it returns
    the entire dataframe.
    Args:
        df: the DataFrame to filter
        start_year: the range of years to filter by, inclusive. If None, no filtering is applied.
        end_year: the range of years to filter by, inclusive. If None, no filtering is applied.

    Returns:
        df: the filtered DataFrame
    """

    available_years = set(df["Commitment Year"].dropna().unique())

    if start_year and (start_year not in available_years):
        raise ValueError(
            f"Provided start year {start_year} is not available in the data. "
            f"Available years are: {available_years}"
        )
    if end_year and (end_year not in available_years):
        raise ValueError(
            f"Provided end year {end_year} is not available in the data. "
            f"Available years are: {available_years}"
        )

    if start_year:
        df = df.loc[lambda d: d["Commitment Year"] >= start_year]
    if end_year:
        df = df.loc[lambda d: d["Commitment Year"] <= end_year]

    return df.reset_index(drop=True)


@cache_info
def download_aiddata(
    save_to_path: Path | str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    pre_process: bool = True,
) -> pd.DataFrame | None:
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
    df = bulk_download_aiddata()

    # Filter years, if needed
    df = filter_years(df=df, start_year=start_year, end_year=end_year)

    # get scheme for dtypes and column names
    schema = read_schema_translation(version="aidData")

    # Convert dtypes
    df = convert_dtypes(df, schema=schema)

    # rename/remove columns, convert bool columns
    if pre_process:
        df = preprocess(df, schema)

    # remove columns where all rows are NaN
    df = df.dropna(axis=1, how="all")

    if save_to_path:
        df.to_parquet(save_to_path)
        return None

    return df
