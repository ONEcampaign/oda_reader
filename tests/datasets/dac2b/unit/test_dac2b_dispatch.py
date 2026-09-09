"""Unit tests for DAC2b dispatch wiring (offline, no network)."""

import pytest

import oda_reader.dac2b as dac2b_module
import oda_reader.download.download_tools as dt
from oda_reader import get_available_filters
from oda_reader.schemas.schema_tools import read_schema_translation


@pytest.mark.unit
class TestDAC2bDispatch:
    """Assert that DAC2b is correctly wired to the DAC2-generic filter/schema machinery."""

    def test_schema_alias_matches_dac2a(self):
        """DAC2b schema alias resolves to the same mapping as DAC2a."""
        assert read_schema_translation("dac2b") == read_schema_translation("dac2a")

    def test_available_filters_match_dac2a(self):
        """get_available_filters('dac2b') returns the same surface as DAC2a."""
        assert get_available_filters("dac2b", quiet=True) == get_available_filters(
            "dac2a", quiet=True
        )

    def test_dataflow_constants(self):
        """DATAFLOW_ID and DATAFLOW_VERSION are set to the confirmed live values."""
        assert dac2b_module.DATAFLOW_ID == "DSD_DAC2@DF_DAC2B"
        assert dac2b_module.DATAFLOW_VERSION == "1.7"

    def test_dac2b_dispatch_uses_dac2_converter(self, mocker):
        """The 'dac2b' dispatch in download() calls convert_dac2_to_dotstat_codes."""
        import pandas as pd

        raw = pd.DataFrame({"x": [1]})

        # Prevent any network call
        mocker.patch.object(dt, "api_response_to_df", return_value=raw)
        # preprocess must accept (df, schema_translation) and return a DataFrame
        mocker.patch.object(
            dt, "preprocess", side_effect=lambda df, schema_translation: df
        )
        # Spy on the DAC2-generic converter to verify it is the one called
        spy = mocker.patch.object(
            dt, "convert_dac2_to_dotstat_codes", side_effect=lambda df: df
        )
        # Bypass the DataFrame cache so the call always reaches the converter
        cache_instance = dt.dataframe_cache()
        mocker.patch.object(cache_instance, "get", return_value=None)
        mocker.patch.object(cache_instance, "set", return_value=None)

        dt.download(
            version="dac2b",
            dataflow_id="DSD_DAC2@DF_DAC2B",
            dataflow_version="1.7",
            pre_process=True,
            dotstat_codes=True,
        )

        spy.assert_called_once()
