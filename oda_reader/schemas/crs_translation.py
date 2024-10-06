import pandas as pd

from oda_reader.common import ImporterPaths
from oda_reader.schemas.dac1_translation import prices_mapping
from oda_reader.schemas.schema_tools import map_area_codes, map_amount_type_codes
from oda_reader.schemas.xml_tools import (
    parse_xml,
    extract_dac_to_area_codes,
    read_mapping,
)

MAPPINGS = {
    "dac2_codes_area": ImporterPaths.mappings / "dac2_codes_area.json",
    "area_code_corrections": ImporterPaths.mappings / "area_code_corrections.json",
}


def area_code_mapping() -> dict:
    """Reads the area code mapping."""
    return read_mapping(
        MAPPINGS["dac2_codes_area"], keys_as_int=True, update=lambda d: d
    ) | read_mapping(
        MAPPINGS["area_code_corrections"], keys_as_int=True, update=lambda d: d
    )


def convert_crs_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the CRS data to the .stat schema.
    Args:
        df (pd.DataFrame): The CRS data.

    Returns:
        pd.DataFrame: The CRS data in the .stat schema.
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
