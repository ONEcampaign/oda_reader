"""Unit tests for the version_discovery module."""

import pytest

from oda_reader.download.version_discovery import (
    _build_metadata_url,
    _parse_version_from_xml,
    clear_version_cache,
    discover_latest_version,
    get_dimension_count,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Structure xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message">
  <Structures>
    <Dataflows>
      <Dataflow id="DF_DAC1" agencyID="OECD.DCD.FSD" version="1.7">
        <Name>DAC1</Name>
      </Dataflow>
    </Dataflows>
  </Structures>
</Structure>
"""

_NAMESPACED_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
               xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
  <mes:Structures>
    <str:Dataflows>
      <str:Dataflow id="DF_DAC1" agencyID="OECD.DCD.FSD" version="2.3">
        <str:Name>DAC1</str:Name>
      </str:Dataflow>
    </str:Dataflows>
  </mes:Structures>
</mes:Structure>
"""

_MISSING_VERSION_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Structure>
  <Structures>
    <Dataflows>
      <Dataflow id="DF_DAC1" agencyID="OECD.DCD.FSD">
        <Name>DAC1</Name>
      </Dataflow>
    </Dataflows>
  </Structures>
</Structure>
"""


# ---------------------------------------------------------------------------
# TestBuildMetadataUrl
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBuildMetadataUrl:
    """Verify URL construction for various dataflow IDs."""

    def test_basic_dataflow_id(self):
        url = _build_metadata_url("DSD_DAC1@DF_DAC1")
        assert url == (
            "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
            "/DSD_DAC1@DF_DAC1/latest"
        )

    def test_crs_dataflow_id(self):
        url = _build_metadata_url("DSD_CRS@DF_CRS")
        assert url == (
            "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
            "/DSD_CRS@DF_CRS/latest"
        )

    def test_multisystem_dataflow_id(self):
        url = _build_metadata_url("DSD_MULTI@DF_MULTI")
        assert url == (
            "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
            "/DSD_MULTI@DF_MULTI/latest"
        )

    def test_dac2a_dataflow_id(self):
        url = _build_metadata_url("DSD_DAC2@DF_DAC2A")
        assert url == (
            "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD"
            "/DSD_DAC2@DF_DAC2A/latest"
        )


# ---------------------------------------------------------------------------
# TestParseVersionFromXml
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseVersionFromXml:
    """Verify XML parsing handles valid, namespaced, and invalid inputs."""

    def test_valid_xml(self):
        version = _parse_version_from_xml(_VALID_XML)
        assert version == "1.7"

    def test_namespaced_xml(self):
        version = _parse_version_from_xml(_NAMESPACED_XML)
        assert version == "2.3"

    def test_missing_version_raises(self):
        with pytest.raises(ValueError, match="No <Dataflow version"):
            _parse_version_from_xml(_MISSING_VERSION_XML)

    def test_malformed_xml_raises(self):
        with pytest.raises(Exception):
            _parse_version_from_xml("<not valid xml >>>")


# ---------------------------------------------------------------------------
# TestDiscoverLatestVersion
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_http(mocker):
    """Patch get_response_text in version_discovery and clear the cache."""
    clear_version_cache()
    return mocker.patch(
        "oda_reader.download.version_discovery.get_response_text",
    )


@pytest.mark.unit
class TestDiscoverLatestVersion:
    """Verify HTTP calls, caching behaviour, and error propagation."""

    def test_returns_parsed_version(self, _mock_http):
        _mock_http.return_value = (200, _VALID_XML, False)

        version = discover_latest_version("DSD_DAC1@DF_DAC1")
        assert version == "1.7"

    def test_result_is_cached(self, _mock_http):
        """A second call for the same dataflow ID must not make another HTTP request."""
        _mock_http.return_value = (200, _VALID_XML, False)

        v1 = discover_latest_version("DSD_DAC1@DF_DAC1")
        v2 = discover_latest_version("DSD_DAC1@DF_DAC1")

        assert v1 == v2 == "1.7"
        _mock_http.assert_called_once()

    def test_http_error_raises_connection_error(self, _mock_http):
        """Non-2xx status code raises ConnectionError."""
        _mock_http.return_value = (404, "Not found", False)

        with pytest.raises(ConnectionError, match="HTTP 404"):
            discover_latest_version("DSD_DAC1@DF_DAC1")

    def test_different_dataflows_cached_separately(self, _mock_http):
        """Each dataflow ID is cached independently."""
        _mock_http.side_effect = [
            (200, _VALID_XML, False),
            (200, _NAMESPACED_XML, False),
        ]

        v_dac1 = discover_latest_version("DSD_DAC1@DF_DAC1")
        v_crs = discover_latest_version("DSD_CRS@DF_CRS")

        assert v_dac1 == "1.7"
        assert v_crs == "2.3"


# ---------------------------------------------------------------------------
# TestClearVersionCache
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestClearVersionCache:
    """Verify that clear_version_cache empties the in-process cache."""

    def test_clear_forces_refetch(self, _mock_http):
        """After clearing, the next call re-fetches from the network."""
        _mock_http.return_value = (200, _VALID_XML, False)

        discover_latest_version("DSD_DAC1@DF_DAC1")
        assert _mock_http.call_count == 1

        clear_version_cache()

        discover_latest_version("DSD_DAC1@DF_DAC1")
        assert _mock_http.call_count == 2

    def test_clear_evicts_http_cache(self, _mock_http, mocker):
        """clear_version_cache should also evict HTTP-cached metadata URLs."""
        _mock_http.return_value = (200, _VALID_XML, False)

        mock_session = mocker.MagicMock()
        mocker.patch(
            "oda_reader.download.version_discovery._get_http_session",
            return_value=mock_session,
        )

        discover_latest_version("DSD_DAC1@DF_DAC1")
        clear_version_cache()

        mock_session.cache.delete.assert_called_once()
        # Verify the URL passed matches the metadata URL pattern
        call_kwargs = mock_session.cache.delete.call_args
        urls = call_kwargs[1]["urls"] if "urls" in call_kwargs[1] else call_kwargs[0][0]
        assert any("DSD_DAC1@DF_DAC1" in u for u in urls)

    def test_clear_on_empty_cache_is_noop(self, mocker):
        """Clearing an empty cache should not error."""
        clear_version_cache()  # should not raise


# ---------------------------------------------------------------------------
# TestGetDimensionCount
# ---------------------------------------------------------------------------

_DSD_XML_7_DIMS = """\
<?xml version="1.0" encoding="UTF-8"?>
<Structure xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
           xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
  <Structures>
    <DataStructures>
      <str:DataStructure id="DSD_DAC1" version="1.7">
        <str:DataStructureComponents>
          <str:DimensionList>
            <str:Dimension id="DONOR" position="1"/>
            <str:Dimension id="SECTOR" position="2"/>
            <str:Dimension id="MEASURE" position="3"/>
            <str:Dimension id="TYING_STATUS" position="4"/>
            <str:Dimension id="FLOW_TYPE" position="5"/>
            <str:Dimension id="UNIT_MEASURE" position="6"/>
            <str:Dimension id="PRICE_BASE" position="7"/>
            <str:TimeDimension id="TIME_PERIOD" position="8"/>
          </str:DimensionList>
        </str:DataStructureComponents>
      </str:DataStructure>
    </DataStructures>
  </Structures>
</Structure>
"""

_DSD_XML_NO_DIMS = """\
<?xml version="1.0" encoding="UTF-8"?>
<Structure xmlns="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
           xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
  <Structures>
    <DataStructures>
      <str:DataStructure id="DSD_DAC1" version="1.0">
        <str:DataStructureComponents>
          <str:DimensionList/>
        </str:DataStructureComponents>
      </str:DataStructure>
    </DataStructures>
  </Structures>
</Structure>
"""


@pytest.mark.unit
class TestGetDimensionCount:
    """Verify DSD dimension counting."""

    def test_counts_dimensions_excluding_time(self, mocker):
        """TimeDimension should not be counted, only Dimension elements."""
        mocker.patch(
            "oda_reader.download.version_discovery.get_response_text",
            return_value=(200, _DSD_XML_7_DIMS, False),
        )

        count = get_dimension_count("DSD_DAC1@DF_DAC1", "1.7")
        assert count == 7

    def test_empty_dsd_raises(self, mocker):
        """DSD with no Dimension elements should raise ValueError."""
        mocker.patch(
            "oda_reader.download.version_discovery.get_response_text",
            return_value=(200, _DSD_XML_NO_DIMS, False),
        )

        with pytest.raises(ValueError, match="No dimensions found"):
            get_dimension_count("DSD_DAC1@DF_DAC1", "1.0")

    def test_http_error_raises(self, mocker):
        """Non-2xx from DSD endpoint should raise ConnectionError."""
        mocker.patch(
            "oda_reader.download.version_discovery.get_response_text",
            return_value=(404, "Not found", False),
        )

        with pytest.raises(ConnectionError, match="HTTP 404"):
            get_dimension_count("DSD_DAC1@DF_DAC1", "9.9")

    def test_splits_dataflow_id_on_at_sign(self, mocker):
        """DSD ID should be derived from the part before '@'."""
        mock = mocker.patch(
            "oda_reader.download.version_discovery.get_response_text",
            return_value=(200, _DSD_XML_7_DIMS, False),
        )

        get_dimension_count("DSD_DAC1@DF_DAC1", "1.7")

        called_url = mock.call_args[0][0]
        assert "/DSD_DAC1/1.7" in called_url
        assert "@" not in called_url


# ---------------------------------------------------------------------------
# TestDiscoverLatestVersionEdgeCases
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscoverLatestVersionEdgeCases:
    """Edge cases for discover_latest_version."""

    def test_bad_xml_in_200_raises_valueerror(self, _mock_http):
        """200 with unparseable XML should raise ValueError."""
        _mock_http.return_value = (200, "<not><valid><xml", False)

        with pytest.raises(Exception):
            discover_latest_version("DSD_DAC1@DF_DAC1")
