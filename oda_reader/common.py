import logging
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

logger = logging.getLogger("oda_importer")


class ImporterPaths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_reader"
    schemas = scripts / "schemas"
    mappings = schemas / "mappings"


def text_to_stringIO(response: requests.models.Response) -> StringIO:
    """Convert the content of a response to bytes.

    Args:
        response (requests.models.Response): The response object from the API.

    Returns:
        StringIO: The content of the response as a stringIO object.

    """
    # Use BytesIO to handle the binary stream data
    return StringIO(response.text)


def get_data_from_api(url: str, compressed: bool = True) -> requests.models.Response:
    """Download a CSV file from an API endpoint and return it as a DataFrame.

    Args:
        url (str): The URL of the API endpoint.
        compressed (bool): Whether the data is fetched compressed. Strongly recommended.

    Returns:
        requests.models.Response: The response object from the API.
    """

    # Set the headers with gzip encoding (if required)
    if compressed:
        headers = {"Accept-Encoding": "gzip"}
    else:
        headers = {}

    # Fetch the data with headers
    logger.info(f"Fetching data from {url}")
    response = requests.get(url, headers=headers)

    if (response.status_code == 404) and (response.text == "NoRecordsFound"):
        raise ConnectionError("No data found for the selected parameters.")

    # Ensure the request was successful
    response.raise_for_status()

    return response


def api_response_to_df(
    url: str, read_csv_options: dict = None, compressed: bool = True
) -> pd.DataFrame:
    """Download a CSV file from an API endpoint and return it as a DataFrame.

    Args:
        url (str): The URL of the API endpoint.
        read_csv_options (dict): Options to pass to `pd.read_csv`.
        compressed (bool): Whether the data is fetched compressed. Strongly recommended.

    Returns:
        pd.DataFrame: The data as a DataFrame.

    """
    # Set default options for read_csv
    if read_csv_options is None:
        read_csv_options = {}

    # If asked for uncompressed data, return the data as is
    if not compressed:
        return pd.read_csv(url, **read_csv_options)

    # Fetch the data from the API with compression headers
    response = get_data_from_api(url=url, compressed=compressed)

    # Convert the content to stringIO
    data = text_to_stringIO(response)

    # Return the data as a DataFrame
    return pd.read_csv(data, **read_csv_options)
