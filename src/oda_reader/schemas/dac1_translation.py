import pandas as pd

from oda_reader.common import ImporterPaths
from oda_reader.schemas.schema_tools import (
    convert_unit_measure_to_amount_type,
    map_amount_type_codes,
    map_area_codes,
)
from oda_reader.schemas.xml_tools import read_mapping

MAPPINGS = {
    "dac1_codes_area": ImporterPaths.mappings / "dac1_codes_area.json",
    "area_code_corrections": ImporterPaths.mappings / "area_code_corrections.json",
    "dac1_codes_prices": ImporterPaths.mappings / "dac1_codes_prices.json",
    "prices_corrections": ImporterPaths.mappings / "code_prices_corrections.json",
    "dac1_codes_flow_types": ImporterPaths.mappings / "dac1_codes_flow_types.json",
}


def area_code_mapping() -> dict:
    """Reads the area code mapping."""
    return read_mapping(
        MAPPINGS["dac1_codes_area"],
        keys_as_int=True,
    ) | read_mapping(
        MAPPINGS["area_code_corrections"],
        keys_as_int=True,
    )


def prices_mapping() -> dict:
    """Reads the prices mapping."""
    return read_mapping(
        MAPPINGS["dac1_codes_prices"],
        keys_as_int=False,
    ) | read_mapping(
        MAPPINGS["prices_corrections"],
        keys_as_int=False,
    )


def flow_types_mapping() -> dict:
    """Reads the flow types mapping."""
    return read_mapping(
        MAPPINGS["dac1_codes_flow_types"],
        keys_as_int=False,
    )


def convert_dac1_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    area_codes = area_code_mapping()
    prices_codes = prices_mapping()

    df = map_area_codes(df, area_code_mapping=area_codes)

    df = convert_unit_measure_to_amount_type(df)
    df = map_amount_type_codes(df, prices_mapping=prices_codes)

    return df
