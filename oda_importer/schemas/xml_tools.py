from xml.etree import ElementTree as ET

import requests

from oda_importer.common import logger


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
