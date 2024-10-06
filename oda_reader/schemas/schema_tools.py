import json

import pandas as pd

from oda_reader.common import logger, ImporterPaths


def read_schema_translation(version: str = "dac1") -> dict:
    """
    Reads the schema translation to map the API response to the .stat schema.

    Args:
        version: The version of the schema to read. Defaults to "dac1".

    Returns:
        dict: The schema translation.
    """
    logger.info(f"Reading the {version} schema translation")

    # Load the schema translation
    with open(ImporterPaths.mappings / f"{version}_dotstat.json", "r") as f:
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


def map_area_codes(
    df: pd.DataFrame,
    area_code_mapping: dict,
    source_column: str = "donor_code",
    target_column: str = "donor_code",
) -> pd.DataFrame:
    """
    Map the new are codes to the old donor codes.

    Args:
        df: A DataFrame containing the new codes.
        area_code_mapping: The mapping between the new and old codes.
        source_column: The column containing the new codes.
        target_column: The column to map the old codes to.

    Returns:
        pd.DataFrame: The DataFrame with the old codes.

    """

    # Swap the keys and values in the dictionary
    donor_codes = {v: k for k, v in area_code_mapping.items()}

    # Map the new codes to the old codes
    df[target_column] = df[source_column].map(donor_codes).astype("int32[pyarrow]")

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
    """Preprocess the DAC1 data.

    Args:
        df (pd.DataFrame): The raw DAC1 data, as returned by the API.
        schema_translation (dict): The schema translation to map the DAC1 API
        response to the .stat schema.

    Returns:
        pd.DataFrame: The preprocessed DAC1 data.

    """
    # Preprocess the data
    logger.info("Preprocessing the data")

    # Get columns to keep
    to_keep = get_columns_to_keep(schema=schema_translation)

    # Get column name mapping
    name_mapping = get_column_name_mapping(schema=schema_translation)

    logger.debug(f"Removing columns: {set(df.columns) - set(to_keep)}")

    # keep only selected columns, rename them
    df = df.filter(items=to_keep).rename(columns=name_mapping)

    return df
