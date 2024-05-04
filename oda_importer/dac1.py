import pandas as pd

from oda_importer.common import api_response_to_df, logger
from oda_importer.query_builder import QueryBuilder
from oda_importer.schemas.dac1_translation import area_code_mapping, prices_mapping
from oda_importer.schemas.schema_tools import (
    dac1_schema_translation,
    get_dtypes,
    get_columns_to_keep,
    get_column_name_mapping,
    map_donor_codes,
    map_amount_type_codes,
    convert_unit_measure_to_amount_type,
)

DATAFLOW_ID: str = "DSD_DAC1@DF_DAC1"


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


def convert_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    # Get the area codes
    area_codes = area_code_mapping()

    # Prices mapping
    prices_codes = prices_mapping()

    # Map the donor codes
    df = map_donor_codes(df, area_code_mapping=area_codes)

    # Map the prices codes
    df = convert_unit_measure_to_amount_type(df)
    df = map_amount_type_codes(df, prices_mapping=prices_codes)

    return df


def download_dac1(
    start_year: int | None = None,
    end_year: int | None = None,
    pre_process: bool = True,
    dotstat_codes: bool = True,
) -> pd.DataFrame:
    """
    Download the DAC1 data from the API.

    Args:
        start_year (int): The start year of the data to download. Optional
        end_year (int): The end year of the data to download. Optional
        pre_process (bool): Whether to preprocess the data. Defaults to True.
        Preprocessing makes it comply with the .stat schema.
        dotstat_codes (bool): Whether to convert the donor codes to the .stat schema.

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

    # get the url
    url = (
        QueryBuilder(dataflow_id=DATAFLOW_ID)
        .set_time_period(start=start_year, end=end_year)
        .build_query()
    )

    # Get the dataframe
    df = api_response_to_df(url=url, read_csv_options=df_options)

    # Preprocess the data
    if pre_process:
        df = preprocess_dac1(df=df, schema_translation=schema_translation)
        if dotstat_codes:
            df = convert_to_dotstat_codes(df)
    else:
        if dotstat_codes:
            raise ValueError("Cannot convert to dotstat codes without preprocessing.")

    # Return the dataframe
    logger.info("Data downloaded correctly.")

    return df
