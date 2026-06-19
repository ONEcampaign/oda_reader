"""Integration tests for CPA dataset."""

import pytest

from oda_reader import download_cpa, enable_http_cache


@pytest.mark.integration
@pytest.mark.slow
class TestCPAIntegration:
    """End-to-end tests for CPA with real API."""

    def test_cpa_basic_query(self):
        """Test CPA raw API query returns project-level data."""
        enable_http_cache()

        # Small query: US CPA data for 2022.
        # CPA defaults to microdata=True (MD_DIM=DD, project-level).
        # Using pre_process=False and dotstat_codes=False to test raw API output.
        df = download_cpa(
            start_year=2022,
            end_year=2022,
            filters={"donor": "USA"},
            pre_process=False,
            dotstat_codes=False,
        )

        assert df is not None
        assert len(df) > 0
        # Raw API columns
        assert "TIME_PERIOD" in df.columns
        assert "OBS_VALUE" in df.columns

    @pytest.mark.slow
    def test_cpa_processed_query(self):
        """Test CPA processed query applies schema translation and dotstat codes."""
        enable_http_cache()

        # Processed path: pre_process=True, dotstat_codes=True (defaults).
        df = download_cpa(
            start_year=2022,
            end_year=2022,
            filters={"donor": "USA"},
        )

        assert df is not None
        assert len(df) > 0
