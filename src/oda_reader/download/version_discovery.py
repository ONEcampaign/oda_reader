"""Dynamic dataflow version discovery via the OECD SDMX metadata endpoint.

This module queries the authoritative SDMX metadata API to determine the
latest published version of a dataflow, replacing the blind version-decrement
fallback strategy.

HTTP calls are made through the project's shared requests-cache session and
are subject to the global rate limiter — both sourced from _http_primitives
to avoid a circular import with common.py.
"""

import logging
import xml.etree.ElementTree as ET

from oda_reader._http_primitives import _get_http_session, get_response_text

logger = logging.getLogger("oda_importer")

METADATA_BASE_URL = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
DSD_BASE_URL = "https://sdmx.oecd.org/public/rest/datastructure/OECD.DCD.FSD"

# In-process cache: dataflow_id -> version string
_version_cache: dict[str, str] = {}


def _build_metadata_url(dataflow_id: str) -> str:
    """Construct the SDMX metadata URL for a given dataflow ID.

    Args:
        dataflow_id: The SDMX dataflow identifier, e.g. ``DSD_DAC1@DF_DAC1``.

    Returns:
        str: The full metadata URL.
    """
    return f"{METADATA_BASE_URL}/{dataflow_id}/latest"


def _parse_version_from_xml(xml_text: str) -> str:
    """Extract the version attribute from SDMX Dataflow XML.

    Iterates over all elements looking for the one whose local name is
    ``Dataflow`` and returns its ``version`` attribute.  The search is
    namespace-agnostic so it works regardless of the XML namespace prefix
    used by the server.

    Args:
        xml_text: Raw XML response text from the metadata endpoint.

    Returns:
        str: The version string, e.g. ``"1.7"``.

    Raises:
        ValueError: If no Dataflow element with a version attribute is found.
    """
    root = ET.fromstring(xml_text)
    for element in root.iter():
        local_name = element.tag.split("}")[-1] if "}" in element.tag else element.tag
        if local_name == "Dataflow" and "version" in element.attrib:
            return element.attrib["version"]
    raise ValueError(
        "No <Dataflow version='...'> element found in SDMX metadata response."
    )


def discover_latest_version(dataflow_id: str) -> str:
    """Query the OECD SDMX metadata endpoint to find the latest dataflow version.

    Results are cached in the module-level ``_version_cache`` dict so that
    repeated calls for the same dataflow ID within a process session incur
    only one network round-trip.

    The HTTP call uses the shared requests-cache session (7-day filesystem
    cache) and the global rate limiter.

    Args:
        dataflow_id: The SDMX dataflow identifier, e.g. ``DSD_DAC1@DF_DAC1``.

    Returns:
        str: The latest version string, e.g. ``"1.7"``.

    Raises:
        ConnectionError: If the metadata endpoint returns a non-2xx status.
        ValueError: If the response XML does not contain a parseable version.
    """
    if dataflow_id in _version_cache:
        return _version_cache[dataflow_id]

    url = _build_metadata_url(dataflow_id)

    status_code, text, _ = get_response_text(url, headers={})

    if status_code > 299:
        raise ConnectionError(
            f"Metadata endpoint returned HTTP {status_code} for "
            f"dataflow '{dataflow_id}': {text[:200]}"
        )

    version = _parse_version_from_xml(text)
    _version_cache[dataflow_id] = version
    logger.info(f"Discovered latest version for '{dataflow_id}': {version}")
    return version


def get_dimension_count(dataflow_id: str, version: str) -> int:
    """Fetch the DSD for a specific version and count key dimensions.

    This excludes the TimeDimension, counting only the dimensions that
    form the positional filter key.

    Args:
        dataflow_id: e.g. ``"DSD_DAC1@DF_DAC1"``.
        version: e.g. ``"1.7"``.

    Returns:
        int: Number of key dimensions.

    Raises:
        ConnectionError: If the DSD endpoint is unreachable.
        ValueError: If no dimensions are found in the response.
    """
    # The DSD ID is the part before '@' in the dataflow ID
    dsd_id = dataflow_id.split("@")[0] if "@" in dataflow_id else dataflow_id
    url = f"{DSD_BASE_URL}/{dsd_id}/{version}"

    status_code, text, _ = get_response_text(url, headers={})

    if status_code > 299:
        raise ConnectionError(
            f"DSD endpoint returned HTTP {status_code} for "
            f"'{dsd_id}' version {version}: {text[:200]}"
        )

    root = ET.fromstring(text)
    count = 0
    for element in root.iter():
        local_name = element.tag.split("}")[-1] if "}" in element.tag else element.tag
        if local_name == "Dimension":
            count += 1
    if count == 0:
        raise ValueError(
            f"No dimensions found in DSD '{dsd_id}' version {version}."
        )
    return count


def clear_version_cache() -> None:
    """Clear both the in-process version cache and any HTTP-cached metadata.

    Call this when you need to force a fresh metadata lookup, for example
    after a new dataflow version has been published mid-session.

    Example:
        >>> from oda_reader import clear_version_cache
        >>> clear_version_cache()
    """
    # Evict HTTP-cached metadata responses so the next lookup hits the network.
    session = _get_http_session()
    for dataflow_id in _version_cache:
        url = _build_metadata_url(dataflow_id)
        session.cache.delete(urls=[url])

    _version_cache.clear()
    logger.info("Version discovery cache cleared.")
