import pandas as pd

from oda_importer.common import api_response_to_df, logger
from oda_importer.query_builder import QueryBuilder
from oda_importer.schemas.schema_tools import (
    dac1_schema_translation,
    get_dtypes,
    get_columns_to_keep,
    get_column_name_mapping,
)

DATAFLOW_ID: str = "DSD_DAC1@DF_DAC1"

DAC1_API_ENDPOINT: str = QueryBuilder(dataflow_id=DATAFLOW_ID).build_query()


def preprocess_dac1(df: pd.DataFrame, schema_translation: dict) -> pd.DataFrame:
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

    # keep only selected columns, rename them
    df = df.filter(items=to_keep).rename(columns=name_mapping)

    return df


def download_dac1() -> pd.DataFrame:
    """
    Download the DAC1 data from the API.

    Returns:
        pd.DataFrame: The DAC1 data.

    """
    # Load the translation schema from .stat  to the new explorer
    schema_translation = dac1_schema_translation()

    # Get a data types dictionary
    data_types = get_dtypes(schema=schema_translation)

    # Set read csv options
    df_options = {
        "na_values": ("_Z", "nan"),
        "keep_default_na": True,
        "dtype": data_types,
    }

    # Inform download is about to start
    logger.info("Downloading DAC1 data. This may take a while...")

    # Get the dataframe
    df = api_response_to_df(url=DAC1_API_ENDPOINT, read_csv_options=df_options)

    # Preprocess the data
    df = preprocess_dac1(df=df, schema_translation=schema_translation)

    # Return the dataframe
    logger.info("Data downloaded correctly.")

    return df
