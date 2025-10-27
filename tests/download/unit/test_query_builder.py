"""Unit tests for QueryBuilder filter construction."""

import pytest

from oda_reader.download.query_builder import QueryBuilder


@pytest.mark.unit
class TestQueryBuilderDAC1:
    """Test DAC1 filter building."""

    def test_dac1_filter_basic(self):
        """Test basic DAC1 filter construction."""
        qb = QueryBuilder(dataflow_id="DF_DAC1", api_version=1)
        result = qb.build_dac1_filter(
            donor="1",
            measure="2010",
            flow_type="1140",
        )

        # Should have donor.measure.untied.flow_type.unit_measure.price_base.period
        # With None values as empty strings for API v1
        assert result == "1.2010..1140..."

    def test_dac1_filter_all_parameters(self):
        """Test DAC1 filter with all parameters specified."""
        qb = QueryBuilder(dataflow_id="DF_DAC1", api_version=1)
        result = qb.build_dac1_filter(
            donor="1",
            measure="2010",
            flow_type="1140",
            unit_measure="USD",
            price_base="V",
        )

        assert result == "1.2010..1140.USD.V."

    def test_dac1_filter_no_parameters(self):
        """Test DAC1 filter with no parameters (all dimensions)."""
        qb = QueryBuilder(dataflow_id="DF_DAC1", api_version=1)
        result = qb.build_dac1_filter()

        # All empty for API v1
        assert result == "......"


@pytest.mark.unit
class TestQueryBuilderDAC2a:
    """Test DAC2a filter building."""

    def test_dac2a_filter_basic(self):
        """Test basic DAC2a filter construction."""
        qb = QueryBuilder(dataflow_id="DF_DAC2A", api_version=1)
        result = qb.build_dac2a_filter(
            donor="1",
            recipient="503",
            measure="1010",
        )

        # donor.recipient.measure.unit_measure.price_base
        assert result == "1.503.1010.."

    def test_dac2a_filter_all_parameters(self):
        """Test DAC2a filter with all parameters."""
        qb = QueryBuilder(dataflow_id="DF_DAC2A", api_version=1)
        result = qb.build_dac2a_filter(
            donor="1",
            recipient="503",
            measure="1010",
            unit_measure="USD",
            price_base="V",
        )

        assert result == "1.503.1010.USD.V"


@pytest.mark.unit
class TestQueryBuilderCRS:
    """Test CRS filter building."""

    @pytest.mark.parametrize(
        "microdata,expected_md_dim",
        [
            (True, "DD"),
            (False, "_T"),
        ],
    )
    def test_crs_filter_microdata_flag(self, microdata, expected_md_dim):
        """Test CRS filter respects microdata flag."""
        qb = QueryBuilder(dataflow_id="DF_CRS", api_version=1)
        result = qb.build_crs_filter(
            donor="1",
            microdata=microdata,
        )

        # Check that md_dim (9th position) matches expected
        parts = result.split(".")
        assert parts[8] == expected_md_dim

    def test_crs_filter_all_dimensions(self):
        """Test CRS filter with all dimensions specified."""
        qb = QueryBuilder(dataflow_id="DF_CRS", api_version=1)
        result = qb.build_crs_filter(
            donor="1",
            recipient="503",
            sector="100",
            measure="1010",
            channel="1",
            modality="M01",
            flow_type="1140",
            price_base="V",
            unit_measure="USD",
            microdata=True,
        )

        expected = "1.503.100.1010.1.M01.1140.V.DD..USD"
        assert result == expected


@pytest.mark.unit
class TestQueryBuilderURL:
    """Test complete URL construction."""

    def test_build_query_api_v1(self):
        """Test complete URL construction for API v1."""
        qb = QueryBuilder(
            dataflow_id="DF_DAC1",
            dataflow_version="1.0",
            api_version=1,
        )
        qb.set_filter("1.2010..1140...")
        qb.set_time_period(start=2020, end=2023)

        url = qb.build_query()

        assert "https://sdmx.oecd.org/public/rest/data/" in url
        assert "OECD.DCD.FSD" in url
        assert "DF_DAC1,1.0/" in url
        assert "1.2010..1140..." in url
        assert "startPeriod=2020" in url
        assert "endPeriod=2023" in url
        assert "format=csvfilewithlabels" in url

    def test_build_query_api_v2(self):
        """Test complete URL construction for API v2."""
        qb = QueryBuilder(
            dataflow_id="DF_DAC1",
            dataflow_version="2.0",
            api_version=2,
        )
        qb.set_filter("*.*.*.*.*.*.*")
        qb.set_time_period(start=2020, end=2023)

        url = qb.build_query()

        assert "https://sdmx.oecd.org/public/rest/v2/data/dataflow/" in url
        assert "OECD.DCD.FSD" in url
        assert "DF_DAC1/2.0/" in url
        assert "*.*.*.*.*.*.*" in url
        assert "c[TIME_PERIOD]=ge:2020+le:2023" in url
        assert "format=csvfilewithlabels" in url

    def test_build_query_crs_base_url(self):
        """Test CRS uses correct base URL."""
        qb = QueryBuilder(
            dataflow_id="DF_CRS",
            dataflow_version="1.0",
            api_version=1,
        )

        url = qb.build_query()

        assert "https://sdmx.oecd.org/dcd-public/rest/data/" in url
