import json
import logging
from collections import defaultdict

import pandas as pd

from oda_reader.common import ImporterPaths

logger = logging.getLogger(__name__)

# CPA reuses the full CRS microdata schema; DAC2A and DAC2B share one
# schema (both live on DSD_DAC2). Aliasing avoids duplicate JSON.
_SCHEMA_ALIASES = {"cpa": "crs", "dac2a": "dac2", "dac2b": "dac2"}


def read_schema_translation(version: str = "dac1") -> dict:
    """
    Reads the schema translation to map the API response to the .stat schema.

    Args:
        version: The version of the schema to read. Defaults to "dac1".

    Returns:
        dict: The schema translation.
    """
    logger.info(f"Reading the {version} schema translation")

    file_version = _SCHEMA_ALIASES.get(version, version)

    schema = "schema" if file_version == "aidData" else "dotstat"

    # Load the schema translation
    with open(ImporterPaths.mappings / f"{file_version}_{schema}.json") as f:
        mapping = json.load(f)

    return mapping


def get_dtypes(schema: dict) -> dict:
    """
    Get the data types from the schema. Used to set the dtypes in the DataFrame.
    Args:
        schema: The schema.

    Returns:
        dict: The data types.

    """
    # Create a dictionary to store the data types
    dtypes = {}

    # Iterate over the schema and get the data types
    for column, settings in schema.items():
        dtypes[column] = settings["type"]

    return dtypes


def convert_dtypes(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Convert the dtypes of a DataFrame given a schema translation.
    Args:
        df: DataFrame to be converted.
        schema: The schema.

    Returns:
        df: DataFrame with converted dtypes

    """

    dtypes = get_dtypes(schema=schema)
    for col in df.columns:
        dtype = dtypes[col]
        df[col] = df[col].astype(dtype)

    return df


def get_column_name_mapping(schema: dict) -> dict:
    """
    Get the column name mapping from the schema.
    Args:
        schema: The schema.

    Returns:
        dict: The column name mapping.

    """
    # Create a dictionary to store the column name mapping
    column_name_mapping = {}

    # Iterate over the schema and get the column name mapping
    for column, settings in schema.items():
        column_name_mapping[column] = settings["name"]

    return column_name_mapping


def get_columns_to_keep(schema: dict) -> list:
    """
    Get the columns to keep from the schema.

    Args:
        schema: The schema.

    Returns:
        list: The columns to keep.

    """
    columns_to_keep = []
    for column, settings in schema.items():
        if settings["keep"]:
            columns_to_keep.append(column)

    return columns_to_keep


def get_bool_columns(schema: dict) -> list[str]:
    """
    Get the columns marked as boolean from the schema.

    Args:
        schema (dict): The schema mapping column names to settings.

    Returns:
        list[str]: The list of columns where "bool" is True.
    """
    return [col for col, settings in schema.items() if settings.get("bool") is True]


def find_area_code_clashes(
    area_code_mapping: dict[int, str], corrections: dict[int, str]
) -> dict[str, list[int]]:
    """
    Find the API area codes that don't resolve to exactly one .Stat code.

    An API code clashes when several .Stat codes map to it and no correction
    picks one of them, or when more than one correction names it.

    Args:
        area_code_mapping: The {.Stat code: API code} mapping, with the
            corrections already merged in.
        corrections: The {.Stat code: API code} corrections. Each one picks the
            .Stat code its API code converts to.

    Returns:
        dict[str, list[int]]: Each clashing API code with its .Stat codes,
            sorted. Empty when nothing clashes.

    """
    candidates: dict[str, set[int]] = defaultdict(set)
    for dotstat_code, api_code in area_code_mapping.items():
        candidates[api_code].add(dotstat_code)

    picked: dict[str, set[int]] = defaultdict(set)
    for dotstat_code, api_code in corrections.items():
        picked[api_code].add(dotstat_code)

    clashes = {}
    for api_code in candidates.keys() | picked.keys():
        codes = candidates[api_code] | picked[api_code]
        if len(picked[api_code]) > 1 or (not picked[api_code] and len(codes) > 1):
            clashes[api_code] = sorted(codes)

    return clashes


def build_area_code_lookup(
    area_code_mapping: dict[int, str], corrections: dict[int, str]
) -> dict[str, int]:
    """
    Build the lookup from API area code to .Stat code.

    When several .Stat codes map to the same API code, the correction for that
    API code decides which one it converts to. The order of entries in the
    mapping never matters.

    Args:
        area_code_mapping: The {.Stat code: API code} mapping, with the
            corrections already merged in.
        corrections: The {.Stat code: API code} corrections.

    Returns:
        dict[str, int]: The {API code: .Stat code} lookup.

    Raises:
        ValueError: If an API code has several .Stat codes and no single
            correction picking one.

    """
    clashes = find_area_code_clashes(area_code_mapping, corrections)
    if clashes:
        details = "; ".join(
            f"{api_code} -> {codes}" for api_code, codes in sorted(clashes.items())
        )
        raise ValueError(
            "These API area codes have more than one .Stat code and no correction "
            f"picking one: {details}. Add an entry to area_code_corrections.json."
        )

    picked = {api_code: dotstat_code for dotstat_code, api_code in corrections.items()}

    # With no clashes, an API code with several .Stat codes has exactly one
    # correction, so all its entries resolve to the picked code in any order.
    return {
        api_code: picked.get(api_code, dotstat_code)
        for dotstat_code, api_code in area_code_mapping.items()
    } | picked


def map_area_codes(
    df: pd.DataFrame,
    area_code_lookup: dict[str, int],
    source_column: str = "donor_code",
    target_column: str = "donor_code",
) -> pd.DataFrame:
    """
    Map API area codes to .Stat area codes.

    API codes with no .Stat code are left empty, and a warning names them.

    Args:
        df: A DataFrame containing API area codes.
        area_code_lookup: The {API code: .Stat code} lookup, as built by
            `build_area_code_lookup`.
        source_column: The column containing the API codes.
        target_column: The column to write the .Stat codes to.

    Returns:
        pd.DataFrame: The DataFrame with the .Stat codes.

    Raises:
        TypeError: If the lookup isn't keyed by API code (str) with .Stat codes
            (int) as values, for example an old {.Stat code: API code} mapping.

    """
    if not all(
        isinstance(api_code, str) and type(dotstat_code) is int
        for api_code, dotstat_code in area_code_lookup.items()
    ):
        raise TypeError(
            "area_code_lookup must map API codes (str) to .Stat codes (int). "
            "Build it with build_area_code_lookup()."
        )

    # Read the source before writing the target: they're usually the same column.
    source = df[source_column]
    dotstat_codes = source.map(area_code_lookup)

    unmapped = source.notna() & dotstat_codes.isna()
    if unmapped.any():
        logger.warning(
            f"{int(unmapped.sum())} rows in '{source_column}' have area codes with "
            f"no .Stat code and were left empty: {sorted(source[unmapped].unique())}"
        )

    df[target_column] = dotstat_codes.astype("int32[pyarrow]")

    return df


def map_amount_type_codes(
    df: pd.DataFrame,
    prices_mapping: dict,
    source_column: str = "amounttype_code",
    target_column: str = "amounttype_code",
) -> pd.DataFrame:
    """
    Map the new aidtype codes to the old codes.

    Args:
        df: The dataframe containing the new codes.
        prices_mapping: The mapping between the new and old codes.
        source_column: The column containing the new codes.
        target_column: The column to map the old codes to.

    Returns:
        pd.DataFrame: The DataFrame with the old codes.

    """

    # Map the new codes to the old codes
    df[target_column] = df[source_column].map(prices_mapping)

    return df


def convert_unit_measure_to_amount_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the unit measure to amount type. This is needed because in
    OECD.Stat the concept of unit measure didn't exist, and all data
    was stored under the amount type concept.

    Args:
        df: A preprocessed DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with the amount type column.

    """
    non_usd = (
        df.loc[lambda d: d["unit_measure_code"] != "USD"]
        .drop(columns=["amounttype_code", "amount_type"])
        .rename(
            columns={
                "unit_measure_code": "amounttype_code",
                "unit_measure_name": "amount_type",
            }
        )
    )

    df = df.loc[lambda d: d.unit_measure_code == "USD"]

    return (
        pd.concat([df, non_usd], ignore_index=True)
        .drop(columns=["unit_measure_code", "unit_measure_name"])
        .drop_duplicates()  # USD can generate duplicates
    )


def preprocess(df: pd.DataFrame, schema_translation: dict) -> pd.DataFrame:
    """Preprocess the DAC1 or aidData data.

    Args:
        df (pd.DataFrame): The raw data.
        schema_translation (dict): The schema translation to map.

    Returns:
        pd.DataFrame: The preprocessed data.

    """
    # Preprocess the data
    logger.info("Preprocessing the data")

    # Get bool columns
    bool_columns = get_bool_columns(schema=schema_translation)
    # Convert dtypes and
    if bool_columns:
        for col in bool_columns:
            df[col] = df[col].map({"Yes": 1, "No": 0}).astype("int8[pyarrow]")

    # Get columns to keep
    to_keep = get_columns_to_keep(schema=schema_translation)

    # Get column name mapping
    name_mapping = get_column_name_mapping(schema=schema_translation)

    logger.debug(f"Removing columns: {set(df.columns) - set(to_keep)}")

    # keep only selected columns, rename them
    df = df.filter(items=to_keep).rename(columns=name_mapping)

    return df
