"""Integration tests for DAC2b dataset."""

import pytest

from oda_reader import download_dac2b, enable_http_cache


@pytest.mark.integration
class TestDAC2bIntegration:
    """End-to-end tests for DAC2b with real API."""

    def test_basic_query(self):
        """Test basic DAC2b query returns valid DataFrame."""
        enable_http_cache()

        # Small query: US OOF/export-credit flows.
        # Filtered on donor only -- a donor/recipient pair can legitimately
        # have no OOF rows for a given year.
        # Using pre_process=False and dotstat_codes=False to test raw API
        df = download_dac2b(
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
