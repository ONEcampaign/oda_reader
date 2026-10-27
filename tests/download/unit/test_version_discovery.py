"""Unit tests for the version_discovery module."""

from pathlib import Path

import pytest
import requests
from requests_cache.policy.expiration import get_url_expiration

from oda_reader import _http_primitives
from oda_reader.download import version_discovery
from oda_reader.download.version_discovery import (
    DataflowStructure,
    _build_metadata_url,
    _parse_dataflow_structure,
    clear_version_cache,
    discover_latest_version,
    resolve_dataflow,
)

_FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "dataflow"

DAC1 = "DSD_DAC1@DF_DAC1"
CRS = "DSD_CRS@DF_CRS"


def _fixture(name: str) -> str:
    return (_FIXTURES_DIR / name).read_text(encoding="utf-8")


def _structure_xml(
    *,
    dataflow_id: str = DAC1,
    ref: str = '<Ref id="DSD_X" version="2.0" class="DataStructure"/>',
    data_structures: str = "",
) -> str:
    """A namespaced SDMX-ML structure message with one dataflow."""
    return f"""\
<?xml version="1.0" encoding="UTF-8"?>
<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
             xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
  <m:Structures>
    <s:Dataflows>
      <s:Dataflow id="{dataflow_id}" agencyID="OECD.DCD.FSD" version="1.9">
        <s:Structure>{ref}</s:Structure>
      </s:Dataflow>
    </s:Dataflows>
    <s:DataStructures>{data_structures}</s:DataStructures>
  </m:Structures>
</m:Structure>
"""


def _dsd(version: str, dimensions: str) -> str:
    return f"""
      <s:DataStructure id="DSD_X" version="{version}">
        <s:DataStructureComponents>
          <s:DimensionList>{dimensions}</s:DimensionList>
        </s:DataStructureComponents>
      </s:DataStructure>"""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseDataflowStructure:
    """Read the dataflow version and its DSD's key dimensions."""

    @pytest.mark.parametrize(
        ("fixture", "dataflow_id", "version", "dimensions"),
        [
            (
                "structure_dac1_latest.xml",
                DAC1,
                "1.8",
                (
                    "DONOR",
                    "SECTOR",
                    "MEASURE",
                    "TYING_STATUS",
                    "FLOW_TYPE",
                    "UNIT_MEASURE",
                    "PRICE_BASE",
                ),
            ),
            (
                "structure_dac1_1.5.xml",
                DAC1,
                "1.5",
                (
                    "DONOR",
                    "MEASURE",
                    "TYING_STATUS",
                    "FLOW_TYPE",
                    "UNIT_MEASURE",
                    "PRICE_BASE",
                    "SECTOR",
                ),
            ),
            (
                "structure_crs_latest.xml",
                CRS,
                "1.7",
                (
                    "DONOR",
                    "RECIPIENT",
                    "SECTOR",
                    "MEASURE",
                    "CHANNEL",
                    "MODALITY",
                    "FLOW_TYPE",
                    "PRICE_BASE",
                    "MD_DIM",
                    "MD_ID",
                    "UNIT_MEASURE",
                ),
            ),
        ],
    )
    def test_real_responses(self, fixture, dataflow_id, version, dimensions):
        """The fixtures also hold AttributeRelationship Dimension refs, which
        the key dimensions exclude."""
        structure = _parse_dataflow_structure(_fixture(fixture), dataflow_id)

        assert structure == DataflowStructure(dataflow_id, version, dimensions)

    def test_orders_by_position_from_referenced_dsd(self):
        """Dimensions follow `position`, from the DSD version the dataflow names."""
        xml = _structure_xml(
            data_structures=_dsd("1.0", '<s:Dimension id="STALE" position="1"/>')
            + _dsd(
                "2.0",
                '<s:Dimension id="B" position="2"/>'
                '<s:TimeDimension id="TIME_PERIOD" position="3"/>'
                '<s:Dimension id="A" position="1"/>',
            ),
        )

        structure = _parse_dataflow_structure(xml, DAC1)

        assert structure.version == "1.9"
        assert structure.dimensions == ("A", "B")

    @pytest.mark.parametrize(
        "xml",
        [
            pytest.param("<not valid xml >>>", id="malformed"),
            pytest.param(_structure_xml(dataflow_id="OTHER"), id="no-dataflow"),
            pytest.param(_structure_xml(ref=""), id="no-structure-ref"),
            # The public host returns this for CRS, the dataflow without its DSD.
            pytest.param(_structure_xml(), id="dsd-missing"),
            pytest.param(_structure_xml(data_structures=_dsd("2.0", "")), id="no-dims"),
            pytest.param(
                _structure_xml(
                    data_structures=_dsd("2.0", '<s:Dimension id="A"/>'),
                ),
                id="no-position",
            ),
        ],
    )
    def test_incomplete_response_raises(self, xml):
        with pytest.raises(ValueError, match=DAC1):
            _parse_dataflow_structure(xml, DAC1)


# ---------------------------------------------------------------------------
# resolve_dataflow
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_http(mocker, monkeypatch):
    """Patch the HTTP layer of version_discovery and start from a fresh process."""
    monkeypatch.setattr(version_discovery, "_structure_cache", {})
    monkeypatch.setattr(version_discovery, "_bypass_http_cache_until_resolved", False)
    get = mocker.patch("oda_reader.download.version_discovery.get_response_text")
    get.return_value = (200, _fixture("structure_dac1_latest.xml"), False)
    return get


@pytest.fixture()
def clock(mocker):
    """Patch the monotonic clock version_discovery reads. Set `.return_value`."""
    return mocker.patch(
        "oda_reader.download.version_discovery.monotonic", return_value=0.0
    )


def _force_refresh_flags(get) -> list[bool]:
    return [call.kwargs["force_refresh"] for call in get.call_args_list]


@pytest.mark.unit
class TestResolveDataflow:
    """URL choice, caching, and error propagation."""

    @pytest.mark.parametrize(
        ("dataflow_id", "version", "url"),
        [
            (
                DAC1,
                None,
                "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
                "/DSD_DAC1@DF_DAC1/latest?references=datastructure",
            ),
            (
                DAC1,
                "1.5",
                "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
                "/DSD_DAC1@DF_DAC1/1.5?references=datastructure",
            ),
            (
                CRS,
                None,
                "https://sdmx.oecd.org/dcd-public/rest/dataflow/OECD.DCD.FSD"
                "/DSD_CRS@DF_CRS/latest?references=datastructure",
            ),
            (
                "DSD_MULTI@DF_MULTI",
                None,
                "https://sdmx.oecd.org/dcd-public/rest/dataflow/OECD.DCD.FSD"
                "/DSD_MULTI@DF_MULTI/latest?references=datastructure",
            ),
            (
                "DSD_CPA@DF_CRS_CPA",
                None,
                "https://sdmx.oecd.org/dcd-public/rest/dataflow/OECD.DCD.FSD"
                "/DSD_CPA@DF_CRS_CPA/latest?references=datastructure",
            ),
        ],
    )
    def test_metadata_url(self, dataflow_id, version, url):
        assert _build_metadata_url(dataflow_id, version) == url

    def test_latest_and_explicit_version_cached_separately(self, mock_http):
        mock_http.side_effect = [
            (200, _fixture("structure_dac1_latest.xml"), False),
            (200, _fixture("structure_dac1_1.5.xml"), False),
        ]

        latest = resolve_dataflow(DAC1)
        pinned = resolve_dataflow(DAC1, "1.5")

        assert (latest.version, pinned.version) == ("1.8", "1.5")
        assert resolve_dataflow(DAC1) is latest
        assert resolve_dataflow(DAC1, "1.5") is pinned
        assert mock_http.call_count == 2

    def test_latest_re_resolved_after_a_day_explicit_kept(self, mock_http, clock):
        """A long-lived process sees a release, and a pinned version stays fixed."""
        resolve_dataflow(DAC1)
        resolve_dataflow(DAC1, "1.8")

        clock.return_value = 86399.0
        resolve_dataflow(DAC1)
        assert mock_http.call_count == 2

        clock.return_value = 86400.0
        resolve_dataflow(DAC1)
        resolve_dataflow(DAC1, "1.8")
        assert mock_http.call_count == 3
        assert mock_http.call_args.args[0] == _build_metadata_url(DAC1, None)

    @pytest.mark.parametrize(
        ("version", "match"),
        [
            (None, "HTTP 404"),
            ("9.9", "no version 9.9. Omit dataflow_version to read the latest"),
        ],
    )
    def test_http_404_raises_connection_error(self, mock_http, version, match):
        mock_http.return_value = (404, "Not found", False)

        with pytest.raises(ConnectionError, match=match):
            resolve_dataflow(DAC1, version)

    def test_discover_latest_version(self, mock_http):
        assert discover_latest_version(DAC1) == "1.8"

    def test_discover_latest_version_raises_value_error_on_bad_xml(self, mock_http):
        """The bulk path catches ConnectionError and ValueError only."""
        mock_http.return_value = (200, "<not><valid><xml", False)

        with pytest.raises(ValueError, match=DAC1):
            discover_latest_version(DAC1)


# ---------------------------------------------------------------------------
# clear_version_cache
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClearVersionCache:
    """The next lookup of each key after a clear bypasses the HTTP cache."""

    def test_fresh_process_uses_http_cache(self, mock_http):
        resolve_dataflow(DAC1)

        assert _force_refresh_flags(mock_http) == [False]

    def test_bypasses_http_cache_once_per_key_in_fresh_process(self, mock_http, clock):
        """A restart empties the in-process cache but not the HTTP cache."""
        mock_http.side_effect = lambda url, **_: (
            200,
            _fixture(
                "structure_crs_latest.xml"
                if CRS in url
                else "structure_dac1_latest.xml"
            ),
            False,
        )
        clear_version_cache()
        resolve_dataflow(DAC1)
        resolve_dataflow(CRS)
        resolve_dataflow(DAC1, "1.5")

        clock.return_value = 86400.0
        resolve_dataflow(DAC1)

        assert _force_refresh_flags(mock_http) == [True, True, True, False]

    def test_refetches_resolved_key_past_http_cache(self, mock_http):
        resolve_dataflow(DAC1)

        clear_version_cache()
        resolve_dataflow(DAC1)
        resolve_dataflow(DAC1)

        assert _force_refresh_flags(mock_http) == [False, True]


# ---------------------------------------------------------------------------
# HTTP cache freshness
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("dataflow_id", "version", "expire_after"),
    [(DAC1, None, 86400), (CRS, None, 86400), (DAC1, "1.5", None)],
)
def test_latest_lookup_expires_after_one_day(
    temp_cache_dir, dataflow_id, version, expire_after
):
    """`None` falls back to the session's 7-day default."""
    session = _http_primitives._get_http_session()
    url = requests.Request("GET", _build_metadata_url(dataflow_id, version)).prepare()

    assert get_url_expiration(url.url, session.settings.urls_expire_after) == (
        expire_after
    )
