"""Integration tests for CRS dataset."""

import pytest

from oda_reader import download_crs, enable_http_cache


@pytest.mark.integration
@pytest.mark.slow
class TestCRSIntegration:
    """End-to-end tests for CRS with real API."""

    def test_crs_microdata_query(self):
        """Test CRS microdata query returns project-level data."""
        enable_http_cache()

        # Small query: US education projects (microdata)
        # Using pre_process=False and dotstat_codes=False to test raw API
        df = download_crs(
            start_year=2023,
            end_year=2023,
            filters={"donor": "USA", "sector": "110"},  # US, Education sector
            pre_process=False,
            dotstat_codes=False,
        )

        assert df is not None
        assert len(df) > 0
        # Raw API columns
        assert "TIME_PERIOD" in df.columns
        assert "OBS_VALUE" in df.columns

    def test_crs_aggregated_query(self):
        """Test CRS aggregated query returns summary data."""
        enable_http_cache()

        # Semi-aggregated data (matches Data Explorer format)
        # Using pre_process=False and dotstat_codes=False to test raw API
        df = download_crs(
            start_year=2023,
            end_year=2023,
            filters={
                "donor": "USA",
                "recipient": "NGA",  # Nigeria
                "microdata": False,
                "channel": "_T",  # Total across channels
                "modality": "_T",  # Total across modalities
            },
            pre_process=False,
            dotstat_codes=False,
        )

        assert df is not None
        assert len(df) > 0
        # Raw API columns
        assert "TIME_PERIOD" in df.columns
        assert "OBS_VALUE" in df.columns
