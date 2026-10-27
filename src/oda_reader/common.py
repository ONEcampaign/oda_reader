import logging
from copy import deepcopy
from io import StringIO
from pathlib import Path

import pandas as pd

from oda_reader import _http_primitives
from oda_reader._http_primitives import (
    API_RATE_LIMITER,
    RateLimiter,
    _get_http_session,
)
from oda_reader._http_primitives import (
    get_response_content as _get_response_content,  # noqa: F401  # re-exported
)
from oda_reader._http_primitives import (
    get_response_text as _get_response_text,
)
from oda_reader._logging import LEGACY_LOGGER_NAME

# Re-exports of rate-limiting primitives. They live in _http_primitives
# to break a circular import with version_discovery; this module is the
# stable public re-export surface.
__all__ = [
    "API_RATE_LIMITER",
    "RateLimiter",
    "api_response_to_df",
    "clear_http_cache",
    "disable_http_cache",
    "enable_http_cache",
    "get_data_from_api",
    "get_http_cache_info",
    "logger",
    "text_to_stringio",
]

# The deprecated `oda_importer` logger, kept for 1.x.
logger = logging.getLogger(LEGACY_LOGGER_NAME)
_logger = logging.getLogger(__name__)

# Response bodies that say the dataflow version in the URL does not exist.
_MISSING_DATAFLOW_PATTERNS = ("Could not find Dataflow", "Could not find DSD")


def enable_http_cache() -> None:
    """Enable HTTP response caching.

    Example:
        >>> from oda_reader import enable_http_cache
        >>> enable_http_cache()
    """
    _http_primitives._CACHE_ENABLED = True
    if _http_primitives._HTTP_SESSION is not None:
        _http_primitives._HTTP_SESSION.cache.clear()


def disable_http_cache() -> None:
    """Disable HTTP response caching (useful for testing or forcing fresh data).

    Example:
        >>> from oda_reader import disable_http_cache
        >>> disable_http_cache()
    """
    _http_primitives._CACHE_ENABLED = False


def clear_http_cache() -> None:
    """Clear all cached HTTP responses.

    Example:
        >>> from oda_reader import clear_http_cache
        >>> clear_http_cache()
    """
    session = _get_http_session()
    session.cache.clear()
    _logger.info("HTTP cache cleared")


def get_http_cache_info() -> dict:
    """Get information about the HTTP cache.

    Returns:
        Dict with cache statistics (response_count, redirects, etc.).

    Example:
        >>> from oda_reader import get_http_cache_info
        >>> info = get_http_cache_info()
        >>> print(f"Cached responses: {info['response_count']}")
    """
    session = _get_http_session()
    return {
        "response_count": len(session.cache.responses),
        "redirects_count": len(session.cache.redirects),
    }


# RateLimiter and API_RATE_LIMITER live in _http_primitives to avoid a
# circular import with version_discovery.py.  Re-exported here for
# backward compatibility.


class ImporterPaths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_reader"
    schemas = scripts / "schemas"
    mappings = schemas / "mappings"
    cache = scripts / ".cache"


def text_to_stringio(response_text: str) -> StringIO:
    """Convert the content of a response to bytes.

    Args:
        response (str): The response text from the API.

    Returns:
        StringIO: The content of the response as a stringIO object.

    """
    # Use BytesIO to handle the binary stream data
    return StringIO(response_text)


def get_data_from_api(url: str, compressed: bool = True) -> str:
    """Download a CSV file from an API endpoint and return it as text.

    Args:
        url: The URL of the API endpoint.
        compressed: Whether the data is fetched compressed. Strongly recommended.

    Returns:
        str: The response text from the API.

    Raises:
        ConnectionError: If the API returns a non-2xx status. The message says
            when no data matches the filters, or when the dataflow version in
            the URL does not exist.
    """
    headers = {"Accept-Encoding": "gzip"} if compressed else {}

    status_code, response, _ = _get_response_text(url, headers=headers)

    if (status_code == 500) and (response.find("not set to") > 0):
        url = url.replace("public", "dcd-public")
        status_code, response, _ = _get_response_text(url, headers=headers)

    if status_code <= 299:
        return response

    _logger.error(f"Error {status_code}: {response}")
    if "NoRecordsFound" in response:
        raise ConnectionError(f"No data matches the filters requested from {url}")
    if any(pattern in response for pattern in _MISSING_DATAFLOW_PATTERNS):
        raise ConnectionError(
            f"Error {status_code}: the dataflow version requested from {url} "
            f"does not exist ({response[:200]}). Call clear_version_cache() to "
            f"look up the latest version again, or pass another dataflow_version."
        )
    raise ConnectionError(f"Error {status_code}: {response}")


def api_response_to_df(
    url: str, read_csv_options: dict | None = None, compressed: bool = True
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
    data = text_to_stringio(response)

    # Return the data as a DataFrame
    try:
        d_ = deepcopy(data)
        return pd.read_csv(d_, **read_csv_options)
    except ValueError:
        read_csv_options["dtype"]["CHANNEL"] = "string[pyarrow]"
        read_csv_options["dtype"]["MODALITY"] = "string[pyarrow]"
        read_csv_options["dtype"]["MD_DIM"] = "string[pyarrow]"
        return pd.read_csv(data, **read_csv_options)
