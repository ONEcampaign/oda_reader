"""Integration tests for DAC2b dataset."""

import pytest

from oda_reader import download_dac2b, enable_http_cache


@pytest.mark.integration
class TestDAC2bIntegration:
    """End-to-end tests for DAC2b with real API."""

    def test_basic_query(self):
        """Test basic DAC2b query returns valid DataFrame on the default path.

        Defaults are `pre_process=True, dotstat_codes=True`. That is the path
        the measure-code conversion bug shipped on: `aidtype_code` must come
        back as .stat numbers (3-digit), never raw API MEASURE numbers
        (4-digit, 2000 + .stat code).
        """
        enable_http_cache()

        # Small query: US OOF/export-credit flows.
        # Filtered on donor only -- a donor/recipient pair can legitimately
        # have no OOF rows for a given year.
        df = download_dac2b(
            start_year=2022,
            end_year=2022,
            filters={"donor": "USA"},
        )

        assert df is not None
        assert len(df) > 0
        # .stat schema columns
        assert "aidtype_code" in df.columns
        assert "value" in df.columns

        aidtype_codes = set(df["aidtype_code"].unique().tolist())
        assert all(code < 1000 for code in aidtype_codes)
        assert all(code not in aidtype_codes for code in (2201, 2204, 2292, 2972))

    def test_raw_query_keeps_api_columns(self):
        """`pre_process=False, dotstat_codes=False` still returns raw API columns."""
        enable_http_cache()

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
