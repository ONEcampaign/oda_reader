"""Live tests asserting every stable bulk-download registry URL is reachable.

`@pytest.mark.network`, opt-in via RUN_NETWORK_TESTS=1 -- the repo's existing
pattern (see tests/codelists/network/test_live.py). This resolves each
dataflow's EXT_RESOURCE label through the real annotation XML and then
confirms the resolved file host answers, using a HEAD request (falling back
to a 1-byte Range GET for hosts that reject HEAD) so it never pulls an actual
multi-GB payload. The point is that the next OECD URL move fails here, in
CI, rather than silently for users.
"""

from __future__ import annotations

import os

import pytest
import requests

from oda_reader._http_primitives import DEFAULT_HEADERS
from oda_reader.crs import CRS_FLOW_URL
from oda_reader.dac1 import DAC1_BULK_LABEL, DAC1_FLOW_URL
from oda_reader.dac2a import DAC2A_BULK_LABEL, DAC2A_FLOW_URL
from oda_reader.dac2b import DAC2B_BULK_LABEL, DAC2B_FLOW_URL
from oda_reader.download.download_tools import get_bulk_file_url
from oda_reader.multisystem import MULTI_FLOW_URL, MULTISYSTEM_BULK_LABEL

pytestmark = pytest.mark.network

# The stable (non-year-specific) bulk-download labels. Year-specific CRS zip
# labels (e.g. "CRS 2024 (dotStat format)") roll every year and aren't part
# of this fixed registry.
REGISTRY: list[tuple[str, str, str]] = [
    ("CRS full parquet", CRS_FLOW_URL, "CRS-Parquet"),
    ("CRS reduced parquet", CRS_FLOW_URL, "CRS-reduced-parquet"),
    ("DAC1 full dataset", DAC1_FLOW_URL, DAC1_BULK_LABEL),
    ("DAC2A full dataset", DAC2A_FLOW_URL, DAC2A_BULK_LABEL),
    ("DAC2B full dataset", DAC2B_FLOW_URL, DAC2B_BULK_LABEL),
    ("Multisystem entire dataset", MULTI_FLOW_URL, MULTISYSTEM_BULK_LABEL),
]


def _skip_if_no_network() -> None:
    if os.environ.get("RUN_NETWORK_TESTS") != "1":
        pytest.skip("set RUN_NETWORK_TESTS=1 to run network tests")


@pytest.mark.network
@pytest.mark.parametrize(
    "name,flow_url,label", REGISTRY, ids=[entry[0] for entry in REGISTRY]
)
def test_registry_url_is_reachable(name: str, flow_url: str, label: str) -> None:
    """Every stable bulk-download label resolves to a URL that answers 200/206."""
    _skip_if_no_network()

    url = get_bulk_file_url(flow_url, label)

    response = requests.head(
        url, headers=DEFAULT_HEADERS, timeout=30, allow_redirects=True
    )
    if response.status_code >= 400:
        # Some hosts serving these files reject HEAD; fall back to a 1-byte
        # Range GET, which still never pulls the real payload.
        response = requests.get(
            url,
            headers={**DEFAULT_HEADERS, "Range": "bytes=0-0"},
            timeout=30,
            stream=True,
        )
        response.close()

    assert response.status_code in (200, 206), (
        f"{name} ({label}) resolved to {url} but returned HTTP {response.status_code}"
    )
