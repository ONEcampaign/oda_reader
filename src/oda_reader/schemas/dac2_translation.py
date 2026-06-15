import pandas as pd

from oda_reader.common import ImporterPaths
from oda_reader.schemas.dac1_translation import prices_mapping
from oda_reader.schemas.schema_tools import map_amount_type_codes, map_area_codes
from oda_reader.schemas.xml_tools import read_mapping

MAPPINGS = {
    "dac2_codes_area": ImporterPaths.mappings / "dac2_codes_area.json",
    "area_code_corrections": ImporterPaths.mappings / "area_code_corrections.json",
}


def area_code_mapping() -> dict:
    """Reads the area code mapping."""
    return read_mapping(
        MAPPINGS["dac2_codes_area"],
        keys_as_int=True,
    ) | read_mapping(
        MAPPINGS["area_code_corrections"],
        keys_as_int=True,
    )


def convert_dac2a_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert the DAC2A data to the .stat schema.

    Args:
        df: The DAC2A data.

    Returns:
        The DAC2A data in the .stat schema.
    """
    area_codes = area_code_mapping()
    prices_codes = prices_mapping()

    df = map_area_codes(df, area_code_mapping=area_codes)

    df = map_area_codes(
        df,
        area_code_mapping=area_codes,
        source_column="recipient_code",
        target_column="recipient_code",
    )

    df = map_amount_type_codes(
        df,
        prices_mapping=prices_codes,
        source_column="data_type_code",
        target_column="data_type_code",
    )

    return df
