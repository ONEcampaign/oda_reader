import json
import xml.etree.ElementTree as ET

import requests

path = ""


def download_xml(xml_url: str) -> requests.models.Response:
    """Download the XML file from OECD.Stat."""
    # Get file with requests
    response = requests.get(xml_url)

    # Check if the request was successful
    response.raise_for_status()

    # Return content
    return response


def xml_to_dict(root):
    """Convert an XML file to a dictionary."""

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

    # Download the XML file
    response = download_xml(xml_url)

    # Parse response content as room XML
    xml_root = ET.fromstring(response.content)

    # Convert the root of the XML to a dictionary
    xml_dict = xml_to_dict(root=xml_root)

    return xml_dict


def dac1_schema_translation() -> dict:
    """Reads the schema translation to map the DAC1 API response to the .stat schema."""
    with open(f"{path}/dac1_dotstat.json", "r") as f:
        mapping = json.load(f)

    return mapping


def get_dtypes(schema: dict) -> dict:
    dtypes = {}
    for column, settings in schema.items():
        dtypes[column] = settings["type"]

    return dtypes


def get_column_name_mapping(schema: dict) -> dict:
    column_name_mapping = {}
    for column, settings in schema.items():
        column_name_mapping[column] = settings["name"]

    return column_name_mapping


def get_columns_to_keep(schema: dict) -> list:
    columns_to_keep = []
    for column, settings in schema.items():
        if settings["keep"]:
            columns_to_keep.append(column)

    return columns_to_keep


def keys_to_int(dictionary: dict) -> dict:
    """Convert dictionary keys to integers."""
    return {int(k): v for k, v in dictionary.items() if k.isdigit()}
