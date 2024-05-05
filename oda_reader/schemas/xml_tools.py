import json
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

from oda_reader.common import logger


def download_xml(xml_url: str) -> requests.models.Response:
    """Download the XML file from OECD.Stat.

    Args:
        xml_url (str): The URL of the XML file.

    Returns:
        requests.models.Response: The response object from the API.
    """
    logger.info(f"Downloading XML file from {xml_url}")

    # Get file with requests
    response = requests.get(xml_url)

    # Check if the request was successful
    response.raise_for_status()

    # Return content
    return response


def xml_to_dict(root) -> dict:
    """Convert an XML file to a dictionary.

    Args:
        root: The root of the XML file.

    Returns:
        dict: The XML file as a dictionary.
    """
    logger.info("Converting XML to dictionary")
    # Create a dictionary to store the XML data
    d = {}

    # Add attributes to the dictionary
    for key, val in root.attrib.items():
        d[f"@{key}"] = val

    # If the root has text, add it to the dictionary
    if root.text and root.text.strip():
        d["#text"] = root.text.strip()

    # Add children to the dictionary
    for child in root:
        # Recursively convert children to dictionaries
        child_dict = xml_to_dict(child)

        # Remove namespace
        tag = child.tag.split("}")[-1]

        # If the tag is already in the dictionary, append the child dictionary
        if tag in d:
            # Check if the tag is already a list
            if not isinstance(d[tag], list):
                d[tag] = [d[tag]]
            # Append the child dictionary
            d[tag].append(child_dict)
        else:
            # Add the child dictionary to the dictionary
            d[tag] = child_dict
    return d


def parse_xml(xml_url: str) -> dict:
    """
    Parse an XML file from OECD.Stat and convert it to a dictionary.

    Args:
        xml_url: The URL of the XML file.

    Returns:
        dict: The XML file as a dictionary.

    """
    # Download the XML file
    response = download_xml(xml_url)

    # Parse response content as room XML
    xml_root = ET.fromstring(response.content)

    # Convert the root of the XML to a dictionary
    xml_dict = xml_to_dict(root=xml_root)

    return xml_dict


def keys_to_int(dictionary: dict) -> dict:
    """Convert dictionary keys to integers.

    Args:
        dictionary: A dictionary which has integer keys as strings.

    Returns:
        dict: The dictionary with integer keys.

    """
    return {int(k): v for k, v in dictionary.items() if k.isdigit()}


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


def representation_to_json(xml_dict, index: int, filename: str) -> None:
    """Pipeline to extract and save a representation mapping to a JSON file."""
    # Get the codes from the XML dictionary
    codes = extract_representation_mapping(xml_dict, index=index)

    # Loop through the codes and add them to the mapping dictionary
    mapping = representation_mapping_to_dict(codes)

    # Save the mapping to a JSON file
    save_dict_to_json(mapping, filename=rf"{filename}")

    # Log the result
    logger.info(f"Saved {filename} to disk.")


def extract_dac_to_area_codes(xml_dict: dict, filename: str) -> None:
    """Extracts the DAC1 codes to Area codes from the XML file."""

    # Convert the representation to a JSON file
    representation_to_json(xml_dict, index=0, filename=filename)


def extract_datatypes_to_prices_codes(xml_dict: dict, filename: str) -> None:
    """Extracts the Datatypes to Prices codes from the XML file."""

    # Convert the representation to a JSON file
    representation_to_json(xml_dict, index=1, filename=filename)


def extract_flowtype_to_flowtype_codes(xml_dict: dict, filename: str) -> None:
    """Extracts the Flowtype to Flowtype codes from the XML file."""

    # Convert the representation to a JSON file
    representation_to_json(xml_dict, index=2, filename=filename)


def read_mapping(mapping_path: str, keys_as_int: bool, update: callable) -> dict:
    # Read the mapping from a JSON file. If it doesn't exist, create it.

    if not Path(mapping_path).exists():
        logger.info(f"Not found, downloading.")
        update()

    with open(mapping_path, "r") as f:
        mapping = json.load(f)

    # Convert keys to integers (if required)
    if keys_as_int:
        mapping = keys_to_int(mapping)

    return mapping
