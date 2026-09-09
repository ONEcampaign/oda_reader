"""Live reachability tests for stable bulk-download registry URLs.

Each test resolves the dataflow's EXT_RESOURCE label through the live
annotation XML, sends a streaming request with ``Range: bytes=0-0``, and
closes the response. Set ``RUN_NETWORK_TESTS=1`` to opt in. Normal pull-request
CI excludes these tests.
"""

from __future__ import annotations

import os

import pytest

from oda_reader._http_primitives import DEFAULT_HEADERS, _get_bulk_stream_session
from oda_reader.common import API_RATE_LIMITER
from oda_reader.crs import CRS_FLOW_URL
from oda_reader.dac1 import DAC1_BULK_LABEL, DAC1_FLOW_URL
from oda_reader.dac2a import DAC2A_BULK_LABEL, DAC2A_FLOW_URL
from oda_reader.dac2b import DAC2B_BULK_LABEL, DAC2B_FLOW_URL
from oda_reader.download.download_tools import (
    _classify_bulk_download_response,
    _get_with_validated_redirects,
    get_bulk_file_url,
)
from oda_reader.exceptions import BulkDownloadChallengeError
from oda_reader.multisystem import MULTI_FLOW_URL, MULTISYSTEM_BULK_LABEL

pytestmark = pytest.mark.network

# Stable bulk-download labels shared across releases. Year-specific CRS zip
# labels, such as "CRS 2024 (dotStat format)", change each year.
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


def _probe_package_style_bulk_request(name: str, label: str, url: str) -> None:
    """Probe OECD's bulk-file host with the package's compatibility headers."""
    request_headers = {
        **DEFAULT_HEADERS,
        "Accept-Encoding": "gzip",
        "Range": "bytes=0-0",
    }
    API_RATE_LIMITER.wait()
    with _get_with_validated_redirects(
        _get_bulk_stream_session(), url, request_headers, timeout=(10, 60)
    ) as response:
        error = _classify_bulk_download_response(response, url=url)
        status_code = response.status_code

    if isinstance(error, BulkDownloadChallengeError):
        ray_id = error.cf_ray or "not supplied"
        pytest.fail(
            f"{name} ({label}) resolved to {url}. OECD returned a Cloudflare "
            f"challenge for the probe request with Ray ID {ray_id}."
        )
    if error is not None:
        pytest.fail(
            f"{name} ({label}) resolved to {url} and returned HTTP "
            f"{error.status_code}. The response preview was {error.body!r}."
        )
    assert status_code in (200, 206), (
        f"Package-style probe accepted an unexpected status {status_code}."
    )


@pytest.mark.network
@pytest.mark.parametrize(
    "name,flow_url,label", REGISTRY, ids=[entry[0] for entry in REGISTRY]
)
def test_registry_url_is_reachable(name: str, flow_url: str, label: str) -> None:
    """Every stable bulk label accepts the package's small Range GET probe."""
    _skip_if_no_network()

    url = get_bulk_file_url(flow_url, label)
    _probe_package_style_bulk_request(name, label, url)
