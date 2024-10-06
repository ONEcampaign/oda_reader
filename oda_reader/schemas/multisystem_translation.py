import pandas as pd

from oda_reader.schemas.dac1_translation import prices_mapping
from oda_reader.schemas.dac2_translation import area_code_mapping
from oda_reader.schemas.schema_tools import map_area_codes, map_amount_type_codes


def convert_multisystem_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Placeholder function to convert the multisystem data to the .stat schema."""

    """Convert the Multisystem data to the .stat schema.
    Args:
        df (pd.DataFrame): The multisystem data.

    Returns:
        pd.DataFrame: The multisystem data in the .stat schema.
    """
    # Get the area codes
    area_codes = area_code_mapping()

    # Prices mapping
    prices_codes = prices_mapping()

    # Map the donor codes
    df = map_area_codes(df, area_code_mapping=area_codes)

    # Map region codes
    df = map_area_codes(
        df,
        area_code_mapping=area_codes,
        source_column="recipient_code",
        target_column="recipient_code",
    )

    # Map the prices codes
    df = map_amount_type_codes(
        df,
        prices_mapping=prices_codes,
        source_column="data_type_code",
        target_column="data_type_code",
    )

    return df
