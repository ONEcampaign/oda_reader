"""Integration tests for DAC1 dataset."""

import pytest

from oda_reader import download_dac1, enable_http_cache


@pytest.mark.integration
class TestDAC1Integration:
    """End-to-end tests for DAC1 with real API."""

    def test_basic_query_returns_dataframe(self):
        """Test basic DAC1 query returns valid DataFrame."""
        enable_http_cache()

        # Very small query: just one year, no specific filters
        # Using pre_process=False and dotstat_codes=False to test raw API
        df = download_dac1(
            start_year=2023,
            end_year=2023,
            pre_process=False,
            dotstat_codes=False,
        )

        assert df is not None
        assert len(df) > 0

        # Raw API columns
        assert "TIME_PERIOD" in df.columns
        assert "OBS_VALUE" in df.columns

    def test_query_with_preprocessing(self):
        """Test DAC1 query with preprocessing enabled."""
        enable_http_cache()

        # Test with preprocessing - should rename columns to .stat schema
        df = download_dac1(
            start_year=2023,
            end_year=2023,
            pre_process=True,
            dotstat_codes=False,  # Only test preprocessing, not code translation
        )

        assert df is not None
        assert len(df) > 0

        # With preprocessing, TIME_PERIOD → year, OBS_VALUE → value
        assert "year" in df.columns
        assert "value" in df.columns

    def test_query_with_dotstat_codes(self):
        """Test DAC1 query with legacy .stat code translation."""
        enable_http_cache()

        # Test with both preprocessing and code translation
        df = download_dac1(
            start_year=2023,
            end_year=2023,
            pre_process=True,
            dotstat_codes=True,
        )

        assert df is not None
        assert len(df) > 0

        # With preprocessing enabled, columns are renamed
        assert "year" in df.columns
        assert "value" in df.columns
