import logging
import re
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
from oda_reader.download.version_discovery import (
    discover_latest_version,
    get_dimension_count,
)

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

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

logger = logging.getLogger("oda_importer")

# Error message patterns that indicate a dataflow/version not found error
# These should trigger a version discovery retry
DATAFLOW_NOT_FOUND_PATTERNS = (
    "Could not find Dataflow",
    "Could not find DSD",
    "Dataflow not found",
    "NoRecordsFound",
)


def _is_dataflow_not_found_error(response: str) -> bool:
    """Check if the response indicates a dataflow/version not found error.

    These errors should trigger a version discovery retry rather than
    being treated as valid data.

    Args:
        response: The response text from the API.

    Returns:
        bool: True if the response indicates a dataflow not found error.
    """
    # Check for known error patterns
    for pattern in DATAFLOW_NOT_FOUND_PATTERNS:
        if pattern in response:
            return True

    # Also check if the response is too short to be valid CSV data
    # A valid CSV response should have at least a header line with multiple columns
    stripped = response.strip()
    is_too_short = len(stripped) < 50 and not stripped.startswith('"')
    looks_like_error = "," not in response and "\n" not in response
    return is_too_short and looks_like_error


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
    logger.info("HTTP cache cleared")


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


def _replace_dataflow_version(url: str, version: str) -> str:
    """Replace the dataflow version in the URL.

    Handles both comma-separated (v1) and slash-separated (v2) patterns.
    """
    # v1 pattern: OECD.DCD.FSD,DATAFLOW_ID,VERSION/
    if re.search(r",(\d+\.\d+)/", url):
        return re.sub(r",(\d+\.\d+)/", f",{version}/", url)
    # v2 pattern: OECD.DCD.FSD/DATAFLOW_ID/VERSION/
    return re.sub(r"/(\d+\.\d+)/", f"/{version}/", url)


def _get_dataflow_version(url: str) -> str | None:
    """Get the dataflow version from the URL.

    Handles both comma-separated (v1) and slash-separated (v2) patterns.

    Returns:
        The version string if found, None otherwise.
    """
    # v1 pattern: ,VERSION/
    match = re.search(r",(\d+\.\d+)/", url)
    if match:
        return match.group(1)
    # v2 pattern: /VERSION/ — must not be a port or protocol fragment
    match = re.search(r"/(\d+\.\d+)/", url)
    return match.group(1) if match else None


def _extract_dataflow_id(url: str) -> str | None:
    """Extract the dataflow ID from an SDMX data URL.

    Handles both URL patterns used by the OECD SDMX API:

    - v1 (comma-separated):  ``…/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,1.8/…``
    - v2 (slash-separated):  ``…/OECD.DCD.FSD/DSD_DAC1@DF_DAC1/1.8/…``

    Args:
        url: A full SDMX data or dataflow URL string.

    Returns:
        The dataflow identifier (e.g. ``"DSD_DAC1@DF_DAC1"``) if found,
        ``None`` if the URL does not match a recognised pattern.
    """
    # v1: AGENCY,DATAFLOW_ID,VERSION
    match = re.search(r"OECD\.DCD\.FSD,([^,/]+),\d+\.\d+", url)
    if match:
        return match.group(1)
    # v2: AGENCY/DATAFLOW_ID/VERSION
    match = re.search(r"OECD\.DCD\.FSD/([^/]+)/\d+\.\d+", url)
    return match.group(1) if match else None


def get_data_from_api(url: str, compressed: bool = True) -> str:
    """Download a CSV file from an API endpoint and return it as text.

    If the initial request returns a "dataflow not found" error, the function
    queries the SDMX metadata endpoint to discover the authoritative latest
    version and retries once with that version.

    Args:
        url: The URL of the API endpoint.
        compressed: Whether the data is fetched compressed. Strongly recommended.

    Returns:
        str: The response text from the API.

    Raises:
        ConnectionError: If the request fails and version discovery cannot help,
            or if the discovered version also fails.
    """
    headers = {"Accept-Encoding": "gzip"} if compressed else {}

    status_code, response, _ = _get_response_text(url, headers=headers)

    # If the response indicates a dataflow-not-found error and the URL contains
    # a version, discover the latest version and retry.  Before retrying we
    # verify that the discovered DSD has the same number of key dimensions as
    # the one this release was built for, so we never silently send filters to
    # an incompatible schema.
    version = _get_dataflow_version(url)
    if _is_dataflow_not_found_error(response) and version is not None:
        dataflow_id = _extract_dataflow_id(url)
        if dataflow_id is not None:
            discovered_version = discover_latest_version(dataflow_id)
            if discovered_version == version:
                raise ConnectionError(
                    f"Dataflow not found and discovered version '{discovered_version}' "
                    f"matches the attempted version. Response: {response[:200]}"
                )

            # Verify structural compatibility: the discovered DSD must have
            # the same number of key dimensions as the version we expected.
            try:
                old_dims = get_dimension_count(dataflow_id, version)
                new_dims = get_dimension_count(dataflow_id, discovered_version)
                if old_dims != new_dims:
                    raise ConnectionError(
                        f"Discovered version {discovered_version} has "
                        f"{new_dims} key dimensions, but version {version} "
                        f"had {old_dims}. This is a breaking schema change. "
                        f"Please upgrade oda_reader."
                    )
            except (ConnectionError, ValueError) as exc:
                if "breaking schema change" in str(exc):
                    raise
                logger.warning(
                    f"Could not verify DSD compatibility: {exc}. "
                    f"Proceeding with discovered version {discovered_version}."
                )

            new_url = _replace_dataflow_version(url=url, version=discovered_version)
            logger.info(
                f"Dataflow not found at version {version}, retrying with "
                f"discovered version {discovered_version}"
            )
            status_code, response, _ = _get_response_text(new_url, headers=headers)
            if _is_dataflow_not_found_error(response) or status_code > 299:
                raise ConnectionError(
                    f"Dataflow not found even after version discovery "
                    f"(tried version {discovered_version}). "
                    f"Response: {response[:200]}"
                )
            return response

    if (status_code == 500) and (response.find("not set to") > 0):
        url = url.replace("public", "dcd-public")
        status_code, response, _ = _get_response_text(url, headers=headers)

    if status_code > 299:
        logger.error(f"Error {status_code}: {response}")
        raise ConnectionError(f"Error {status_code}: {response}")

    return response


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
