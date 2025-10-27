"""Integration tests for DAC2a dataset."""

import pytest

from oda_reader import download_dac2a, enable_http_cache


@pytest.mark.integration
class TestDAC2aIntegration:
    """End-to-end tests for DAC2a with real API."""

    def test_basic_query(self):
        """Test basic DAC2a query returns valid DataFrame."""
        enable_http_cache()

        # Small query: US bilateral flows to Kenya
        # Using pre_process=False and dotstat_codes=False to test raw API
        df = download_dac2a(
            start_year=2023,
            end_year=2023,
            filters={"donor": "USA", "recipient": "KEN"},  # US to Kenya
            pre_process=False,
            dotstat_codes=False,
        )

        assert df is not None
        assert len(df) > 0
        # Raw API columns
        assert "TIME_PERIOD" in df.columns
        assert "OBS_VALUE" in df.columns
