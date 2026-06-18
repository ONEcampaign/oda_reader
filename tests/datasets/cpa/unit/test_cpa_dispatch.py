"""Unit tests for CPA dispatch wiring (offline, no network)."""

import pytest

import oda_reader.cpa as cpa_module
import oda_reader.download.download_tools as dt
from oda_reader import get_available_filters
from oda_reader.schemas.schema_tools import read_schema_translation


@pytest.mark.unit
class TestCPADispatch:
    """Assert that CPA is correctly wired to the CRS filter/schema machinery."""

    def test_schema_alias_matches_crs(self):
        """CPA schema alias resolves to the same mapping as CRS."""
        assert read_schema_translation("cpa") == read_schema_translation("crs")

    def test_available_filters_match_crs(self):
        """get_available_filters('cpa') returns the same surface as CRS."""
        assert get_available_filters("cpa", quiet=True) == get_available_filters(
            "crs", quiet=True
        )

    def test_dataflow_constants(self):
        """DATAFLOW_ID and DATAFLOW_VERSION are set to the confirmed live values."""
        assert cpa_module.DATAFLOW_ID == "DSD_CPA@DF_CRS_CPA"
        assert cpa_module.DATAFLOW_VERSION == "1.4"

    def test_cpa_dispatch_uses_crs_converter(self, mocker):
        """The 'cpa' dispatch in download() calls convert_crs_to_dotstat_codes."""
        import pandas as pd

        raw = pd.DataFrame({"x": [1]})

        # Prevent any network call
        mocker.patch.object(dt, "api_response_to_df", return_value=raw)
        # preprocess must accept (df, schema_translation) and return a DataFrame
        mocker.patch.object(
            dt, "preprocess", side_effect=lambda df, schema_translation: df
        )
        # Spy on the CRS converter to verify it is the one called
        spy = mocker.patch.object(
            dt, "convert_crs_to_dotstat_codes", side_effect=lambda df: df
        )
        # Bypass the DataFrame cache so the call always reaches the converter
        cache_instance = dt.dataframe_cache()
        mocker.patch.object(cache_instance, "get", return_value=None)
        mocker.patch.object(cache_instance, "set", return_value=None)

        dt.download(
            version="cpa",
            dataflow_id="DSD_CPA@DF_CRS_CPA",
            dataflow_version="1.4",
            pre_process=True,
            dotstat_codes=True,
        )

        spy.assert_called_once()
