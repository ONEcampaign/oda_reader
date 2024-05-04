import json
from pathlib import Path

from oda_importer.schemas.xml_tools import parse_xml, keys_to_int
from oda_importer.common import logger, ImporterPaths

MAPPINGS = {
    "dac1_codes_area": ImporterPaths.schemas / "dac1_codes_area.json",
    "dac1_codes_prices": ImporterPaths.schemas / "dac1_codes_prices.json",
    "dac1_codes_flow_types": ImporterPaths.schemas / "dac1_codes_flow_types.json",
}

DAC1_TRANSLATION_SCHEMA_URL = (
    "https://stats.oecd.org/FileView2.aspx?IDFile=b9613c8b-b31d-4fd9-ba95-5f129729d693"
)


def save_dict_to_json(dictionary: dict, filename: str) -> None:
    """Saves a dictionary to a JSON file."""
    # Save the mapping to a JSON file
    with open(rf"{filename}", "w") as f:
        f.write(json.dumps(dictionary, indent=4))


def extract_representation_mapping(xml_dict: dict, index: int) -> list:
    """Extracts the representation mapping from the XML file."""
    return xml_dict["RepresentationMaps"]["RepresentationMap"][index][
        "RepresentationMapping"
    ]


def representation_mapping_to_dict(representation_mapping: list) -> dict:
    """Converts the representation mapping to a dictionary."""
    return {
        code["SourceValue"]["#text"]: code["TargetValue"]["#text"]
        for code in representation_mapping
    }


def _representation_to_json(xml_dict, index: int, filename: str) -> None:
    """Pipeline to extract and save a representation mapping to a JSON file."""
    # Get the codes from the XML dictionary
    codes = extract_representation_mapping(xml_dict, index=index)

    # Loop through the codes and add them to the mapping dictionary
    mapping = representation_mapping_to_dict(codes)

    # Save the mapping to a JSON file
    save_dict_to_json(mapping, filename=rf"{filename}")

    # Log the result
    logger.info(f"Saved {filename} to disk.")


def extract_dac_to_area_codes(xml_dict: dict) -> None:
    """Extracts the DAC1 codes to Area codes from the XML file."""

    # Convert the representation to a JSON file
    _representation_to_json(xml_dict, index=0, filename=MAPPINGS["dac1_codes_area"])


def extract_datatypes_to_prices_codes(xml_dict: dict) -> None:
    """Extracts the Datatypes to Prices codes from the XML file."""

    # Convert the representation to a JSON file
    _representation_to_json(xml_dict, index=1, filename=MAPPINGS["dac1_codes_prices"])


def extract_flowtype_to_flowtype_codes(xml_dict: dict) -> None:
    """Extracts the Flowtype to Flowtype codes from the XML file."""

    # Convert the representation to a JSON file
    _representation_to_json(
        xml_dict, index=2, filename=MAPPINGS["dac1_codes_flow_types"]
    )


def update_dac1_translation_mappings():
    """Pipeline to update the DAC1 translation mappings"""
    xml_data = parse_xml(xml_url=DAC1_TRANSLATION_SCHEMA_URL)["Structures"]

    # price mapping
    extract_datatypes_to_prices_codes(xml_dict=xml_data)

    # flow types mapping
    extract_flowtype_to_flowtype_codes(xml_dict=xml_data)

    # oecd dac donor codes to area codes
    extract_dac_to_area_codes(xml_dict=xml_data)


def _read_mapping(mapping: str, keys_as_int: bool = False) -> dict:
    # Read the mapping from a JSON file. If it doesn't exist, create it.

    if not Path(MAPPINGS[mapping]).exists():
        logger.info(f"Not found, downloading.")
        update_dac1_translation_mappings()

    with open(MAPPINGS[mapping], "r") as f:
        mapping = json.load(f)

    # Convert keys to integers (if required)
    if keys_as_int:
        mapping = keys_to_int(mapping)

    return mapping


def area_code_mapping() -> dict:
    """Reads the area code mapping."""
    return _read_mapping("dac1_codes_area", keys_as_int=True)


def prices_mapping() -> dict:
    """Reads the prices mapping."""
    return _read_mapping("dac1_codes_prices", keys_as_int=False)


def flow_types_mapping() -> dict:
    """Reads the flow types mapping."""
    return _read_mapping("dac1_codes_flow_types", keys_as_int=False)
