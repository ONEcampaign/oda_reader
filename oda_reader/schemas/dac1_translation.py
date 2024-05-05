import pandas as pd

from oda_reader.common import ImporterPaths
from oda_reader.schemas.schema_tools import (
    map_area_codes,
    convert_unit_measure_to_amount_type,
    map_amount_type_codes,
)
from oda_reader.schemas.xml_tools import (
    parse_xml,
    extract_dac_to_area_codes,
    extract_datatypes_to_prices_codes,
    extract_flowtype_to_flowtype_codes,
    read_mapping,
)

MAPPINGS = {
    "dac1_codes_area": ImporterPaths.mappings / "dac1_codes_area.json",
    "area_code_corrections": ImporterPaths.mappings / "area_code_corrections.json",
    "dac1_codes_prices": ImporterPaths.mappings / "dac1_codes_prices.json",
    "prices_corrections": ImporterPaths.mappings / "code_prices_corrections.json",
    "dac1_codes_flow_types": ImporterPaths.mappings / "dac1_codes_flow_types.json",
}

DAC1_TRANSLATION_SCHEMA_URL = (
    "https://stats.oecd.org/FileView2.aspx?IDFile=b9613c8b-b31d-4fd9-ba95-5f129729d693"
)


def update_dac1_translation_mappings():
    """Pipeline to update the DAC1 translation mappings"""
    xml_data = parse_xml(xml_url=DAC1_TRANSLATION_SCHEMA_URL)["Structures"]

    # price mapping
    extract_datatypes_to_prices_codes(
        xml_dict=xml_data, filename=MAPPINGS["dac1_codes_prices"]
    )

    # flow types mapping
    extract_flowtype_to_flowtype_codes(
        xml_dict=xml_data, filename=MAPPINGS["dac1_codes_flow_types"]
    )

    # oecd dac donor codes to area codes
    extract_dac_to_area_codes(xml_dict=xml_data, filename=MAPPINGS["dac1_codes_area"])


def area_code_mapping() -> dict:
    """Reads the area code mapping."""
    return read_mapping(
        MAPPINGS["dac1_codes_area"],
        keys_as_int=True,
        update=update_dac1_translation_mappings,
    ) | read_mapping(
        MAPPINGS["area_code_corrections"],
        keys_as_int=True,
        update=update_dac1_translation_mappings,
    )


def prices_mapping() -> dict:
    """Reads the prices mapping."""
    return read_mapping(
        MAPPINGS["dac1_codes_prices"],
        keys_as_int=False,
        update=update_dac1_translation_mappings,
    ) | read_mapping(
        MAPPINGS["prices_corrections"],
        keys_as_int=False,
        update=update_dac1_translation_mappings,
    )


def flow_types_mapping() -> dict:
    """Reads the flow types mapping."""
    return read_mapping(
        MAPPINGS["dac1_codes_flow_types"],
        keys_as_int=False,
        update=update_dac1_translation_mappings,
    )


def convert_dac1_to_dotstat_codes(df: pd.DataFrame) -> pd.DataFrame:
    # Get the area codes
    area_codes = area_code_mapping()

    # Prices mapping
    prices_codes = prices_mapping()

    # Map the donor codes
    df = map_area_codes(df, area_code_mapping=area_codes)

    # Map the prices codes
    df = convert_unit_measure_to_amount_type(df)
    df = map_amount_type_codes(df, prices_mapping=prices_codes)

    return df
