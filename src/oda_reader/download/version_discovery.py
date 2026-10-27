"""Dataflow resolution via the OECD SDMX metadata endpoint.

One request per dataflow, ``{host}dataflow/OECD.DCD.FSD/{dataflow_id}/{version}
?references=datastructure``, returns the dataflow's version and the data
structure it references. :func:`resolve_dataflow` reads both, so a caller
knows which version it is reading and the order of that version's key
dimensions.

HTTP calls are made through the project's shared requests-cache session and
are subject to the global rate limiter — both sourced from _http_primitives
to avoid a circular import with common.py.
"""

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from time import monotonic

from oda_reader._http_primitives import get_response_text

logger = logging.getLogger(__name__)

AGENCY_ID = "OECD.DCD.FSD"
PUBLIC_REST_HOST = "https://sdmx.oecd.org/public/rest/"
DCD_PUBLIC_REST_HOST = "https://sdmx.oecd.org/dcd-public/rest/"


@dataclass(frozen=True)
class DataflowStructure:
    """A resolved dataflow version and its key dimensions.

    Attributes:
        dataflow_id: The SDMX dataflow identifier, e.g. ``DSD_DAC1@DF_DAC1``.
        version: The dataflow version the server resolved, e.g. ``"1.8"``.
        dimensions: Key dimension IDs in key position order, excluding the
            time dimension.
    """

    dataflow_id: str
    version: str
    dimensions: tuple[str, ...]


# A latest lookup is re-resolved after a day, matching its HTTP cache expiry,
# so a long-lived process sees a new release.
_LATEST_TTL_SECONDS = 86400

# In-process cache: (dataflow_id, requested version or None for latest) to
# the structure and the monotonic time it was resolved.
_structure_cache: dict[tuple[str, str | None], tuple[DataflowStructure, float]] = {}

# Set by clear_version_cache. The first lookup of each key after the clear
# bypasses the HTTP cache. A key resolved since then is in _structure_cache.
_bypass_http_cache_until_resolved = False


def sdmx_rest_host(dataflow_id: str) -> str:
    """Return the SDMX REST root that serves a dataflow's data and structure.

    CRS, CPA and Multisystem dataflows live on the ``dcd-public`` host. The
    ``public`` host returns their dataflows without the data structure and
    serves no data for them.

    Args:
        dataflow_id: The SDMX dataflow identifier, e.g. ``DSD_CRS@DF_CRS``.

    Returns:
        str: The REST root, ending in ``/``.
    """
    if "CRS" in dataflow_id or "MULTI" in dataflow_id:
        return DCD_PUBLIC_REST_HOST
    return PUBLIC_REST_HOST


def _build_metadata_url(dataflow_id: str, version: str | None) -> str:
    """Return the dataflow URL that also returns the referenced data structure."""
    return (
        f"{sdmx_rest_host(dataflow_id)}dataflow/{AGENCY_ID}/{dataflow_id}"
        f"/{version or 'latest'}?references=datastructure"
    )


def _local_name(element: ET.Element) -> str:
    """Return an element's tag without its XML namespace."""
    return element.tag.rpartition("}")[2]


def _find(parent: ET.Element, name: str, **attrib: str) -> ET.Element | None:
    """Return the first descendant with local name *name* and attributes *attrib*."""
    for element in parent.iter():
        if _local_name(element) == name and all(
            element.get(key) == value for key, value in attrib.items()
        ):
            return element
    return None


def _parse_dataflow_structure(xml_text: str, dataflow_id: str) -> DataflowStructure:
    """Read the dataflow version and its key dimensions from a metadata response.

    The dimensions come from the ``DimensionList`` of the data structure the
    dataflow's ``Structure/Ref`` names. ``Dimension`` elements elsewhere (for
    example inside ``AttributeRelationship``) refer to dimensions and are
    ignored.

    Args:
        xml_text: SDMX-ML 2.1 structure message.
        dataflow_id: The dataflow requested, used to find it and in errors.

    Returns:
        DataflowStructure: The dataflow version and ordered dimension IDs.

    Raises:
        ValueError: If the XML is malformed, or lacks the dataflow, its data
            structure, or key dimensions with ``id`` and ``position``.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as error:
        raise ValueError(
            f"Metadata response for dataflow '{dataflow_id}' is not valid XML."
        ) from error

    dataflow = _find(root, "Dataflow", id=dataflow_id)
    if dataflow is None or "version" not in dataflow.attrib:
        raise ValueError(
            f"Metadata response has no versioned dataflow '{dataflow_id}'."
        )
    version = dataflow.attrib["version"]

    structure = _find(dataflow, "Structure")
    ref = _find(structure, "Ref") if structure is not None else None
    if ref is None or "id" not in ref.attrib or "version" not in ref.attrib:
        raise ValueError(f"Dataflow '{dataflow_id}' {version} names no data structure.")

    dsd = _find(
        root, "DataStructure", id=ref.attrib["id"], version=ref.attrib["version"]
    )
    dimension_list = _find(dsd, "DimensionList") if dsd is not None else None
    if dimension_list is None:
        raise ValueError(
            f"Metadata response for dataflow '{dataflow_id}' {version} lacks the "
            f"dimensions of data structure '{ref.attrib['id']}' "
            f"{ref.attrib['version']}."
        )

    positioned: list[tuple[int, str]] = []
    for element in dimension_list:
        if _local_name(element) != "Dimension":
            continue
        if "id" not in element.attrib or "position" not in element.attrib:
            raise ValueError(
                f"Data structure '{ref.attrib['id']}' {ref.attrib['version']} of "
                f"dataflow '{dataflow_id}' has a dimension without an id or position."
            )
        positioned.append((int(element.attrib["position"]), element.attrib["id"]))

    if not positioned:
        raise ValueError(
            f"Data structure '{ref.attrib['id']}' {ref.attrib['version']} of "
            f"dataflow '{dataflow_id}' has no key dimensions."
        )

    return DataflowStructure(
        dataflow_id=dataflow_id,
        version=version,
        dimensions=tuple(dimension_id for _, dimension_id in sorted(positioned)),
    )


def resolve_dataflow(dataflow_id: str, version: str | None = None) -> DataflowStructure:
    """Resolve a dataflow version and the order of its key dimensions.

    Results are cached in-process by ``(dataflow_id, version)``. An explicit
    version stays for the life of the process and the latest for one day. The HTTP
    cache keeps a ``latest`` lookup for one day and an explicit-version lookup
    for seven. Call :func:`clear_version_cache` to see a release published
    within the day.

    Args:
        dataflow_id: The SDMX dataflow identifier, e.g. ``DSD_DAC1@DF_DAC1``.
        version: The dataflow version to read, e.g. ``"1.5"``. ``None`` reads
            the latest published version.

    Returns:
        DataflowStructure: The resolved version and its dimension IDs in key
        position order.

    Raises:
        ConnectionError: If the metadata endpoint returns a non-2xx status,
            including a 404 for an explicit version the OECD does not publish.
        ValueError: If the response lacks the dataflow or its key dimensions.
    """
    key = (dataflow_id, version)
    cached = _structure_cache.get(key)
    if cached is not None:
        structure, resolved_at = cached
        if version is not None or monotonic() - resolved_at < _LATEST_TTL_SECONDS:
            return structure

    url = _build_metadata_url(dataflow_id, version)
    status_code, text, _ = get_response_text(
        url,
        headers={},
        force_refresh=_bypass_http_cache_until_resolved and cached is None,
    )

    if status_code == 404 and version is not None:
        raise ConnectionError(
            f"Dataflow '{dataflow_id}' has no version {version}. Omit "
            "dataflow_version to read the latest version."
        )
    if status_code > 299:
        raise ConnectionError(
            f"Metadata endpoint returned HTTP {status_code} for "
            f"dataflow '{dataflow_id}' version {version or 'latest'}: {text[:200]}"
        )

    structure = _parse_dataflow_structure(text, dataflow_id)
    _structure_cache[key] = (structure, monotonic())
    logger.info(
        f"Resolved dataflow '{dataflow_id}' {version or 'latest'} to "
        f"{structure.version}"
    )
    return structure


def discover_latest_version(dataflow_id: str) -> str:
    """Return the latest published version of a dataflow.

    Args:
        dataflow_id: The SDMX dataflow identifier, e.g. ``DSD_DAC1@DF_DAC1``.

    Returns:
        str: The latest version string, e.g. ``"1.7"``.

    Raises:
        ConnectionError: If the metadata endpoint returns a non-2xx status.
        ValueError: If the response lacks the dataflow or its key dimensions.
    """
    return resolve_dataflow(dataflow_id).version


def clear_version_cache() -> None:
    """Clear the in-process dataflow cache and bypass HTTP-cached metadata.

    After the call, the next lookup of each dataflow and version reaches the
    OECD, whether or not this process resolved it before. Later lookups use
    the caches as usual. Call this to see a new dataflow version published
    within the day.

    Example:
        >>> from oda_reader import clear_version_cache
        >>> clear_version_cache()
    """
    global _bypass_http_cache_until_resolved

    _structure_cache.clear()
    _bypass_http_cache_until_resolved = True
    logger.info("Version discovery cache cleared.")
