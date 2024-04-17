import json
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from oda_importer.common import logger, ImporterPaths
from oda_importer.schemas.dac1_translation import area_code_mapping, prices_mapping


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


def dac1_schema_translation() -> dict:
    """
    Reads the schema translation to map the DAC1 API response to the .stat schema.

    Returns:
        dict: The schema translation.
    """
    logger.info("Reading the DAC1 schema translation")

    # Load the schema translation
    with open(ImporterPaths.schemas / "dac1_dotstat.json", "r") as f:
        mapping = json.load(f)

    return mapping


def get_dtypes(schema: dict) -> dict:
    """
    Get the data types from the schema. Used to set the dtypes in the DataFrame.
    Args:
        schema: The schema.

    Returns:
        dict: The data types.

    """
    # Create a dictionary to store the data types
    dtypes = {}

    # Iterate over the schema and get the data types
    for column, settings in schema.items():
        dtypes[column] = settings["type"]

    return dtypes


def get_column_name_mapping(schema: dict) -> dict:
    """
    Get the column name mapping from the schema.
    Args:
        schema: The schema.

    Returns:
        dict: The column name mapping.

    """
    # Create a dictionary to store the column name mapping
    column_name_mapping = {}

    # Iterate over the schema and get the column name mapping
    for column, settings in schema.items():
        column_name_mapping[column] = settings["name"]

    return column_name_mapping


def get_columns_to_keep(schema: dict) -> list:
    """
    Get the columns to keep from the schema.

    Args:
        schema: The schema.

    Returns:
        list: The columns to keep.

    """
    columns_to_keep = []
    for column, settings in schema.items():
        if settings["keep"]:
            columns_to_keep.append(column)

    return columns_to_keep


def keys_to_int(dictionary: dict) -> dict:
    """Convert dictionary keys to integers.

    Args:
        dictionary: A dictionary which has integer keys as strings.

    Returns:
        dict: The dictionary with integer keys.

    """
    return {int(k): v for k, v in dictionary.items() if k.isdigit()}


def map_donor_codes(
    df: pd.DataFrame,
    source_column: str = "donor_code",
    target_column: str = "donor_code",
) -> pd.DataFrame:
    """
    Map the new are codes to the old donor codes.

    Args:
        df: A DataFrame containing the new codes.
        source_column: The column containing the new codes.
        target_column: The column to map the old codes to.

    Returns:
        pd.DataFrame: The DataFrame with the old codes.

    """

    # Swap the keys and values in the dictionary
    donor_codes = {v: k for k, v in area_code_mapping().items()}

    # Map the new codes to the old codes
    df[target_column] = df[source_column].map(donor_codes).astype("int32[pyarrow]")

    return df


def map_aidtype_codes(
    df: pd.DataFrame,
    source_column: str = "aidtype_code",
    target_column: str = "aidtype_code",
) -> pd.DataFrame:
    """
    Map the new aidtype codes to the old codes.

    Args:
        df: The dataframe containing the new codes.
        source_column: The column containing the new codes.
        target_column: The column to map the old codes to.

    Returns:
        pd.DataFrame: The DataFrame with the old codes.

    """

    # Old codes mapping
    aidtype_codes = prices_mapping()

    # Map the new codes to the old codes
    df[target_column] = df[source_column].map(aidtype_codes)

    return df
