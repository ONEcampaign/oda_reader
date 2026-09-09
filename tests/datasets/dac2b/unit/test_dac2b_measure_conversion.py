"""Unit tests for the DAC2B measure-code conversion.

DAC2B's API `MEASURE` codes are not already in .stat numbering (unlike
DAC2A's), so `download_dac2b`'s default path must remap `aidtype_code` from
API numbering to .stat numbering. These pin that behaviour, including the
failure mode for a code the mapping doesn't know about.
"""

import pandas as pd
import pytest

from oda_reader.schemas.dac2_translation import (
    MAPPINGS,
    convert_dac2b_to_dotstat_codes,
    dac2b_measure_mapping,
)

ALL_API_CODES = [
    2201,
    2204,
    2205,
    2217,
    2250,
    2255,
    2292,
    2293,
    2295,
    2296,
    2297,
    2298,
    2972,
]


@pytest.mark.unit
class TestConvertDac2bToDotstatCodes:
    """`convert_dac2b_to_dotstat_codes` remaps `aidtype_code` after the generic conversion."""

    def test_maps_all_known_api_codes_to_dotstat_codes(self, mocker):
        """Every documented API MEASURE code converts to its .stat code."""
        mocker.patch(
            "oda_reader.schemas.dac2_translation.convert_dac2_to_dotstat_codes",
            side_effect=lambda df: df,
        )
        df = pd.DataFrame({"aidtype_code": ALL_API_CODES})

        result = convert_dac2b_to_dotstat_codes(df)

        expected = [code - 2000 for code in ALL_API_CODES]
        assert result["aidtype_code"].tolist() == expected

    def test_runs_generic_conversion_first(self, mocker):
        """The shared DAC2-generic conversion runs before the measure remap."""
        spy = mocker.patch(
            "oda_reader.schemas.dac2_translation.convert_dac2_to_dotstat_codes",
            side_effect=lambda df: df,
        )
        df = pd.DataFrame({"aidtype_code": [2201]})

        convert_dac2b_to_dotstat_codes(df)

        spy.assert_called_once()

    def test_raises_value_error_on_unmapped_code(self, mocker):
        """An API MEASURE code absent from the mapping raises, it doesn't yield NaN."""
        mocker.patch(
            "oda_reader.schemas.dac2_translation.convert_dac2_to_dotstat_codes",
            side_effect=lambda df: df,
        )
        df = pd.DataFrame({"aidtype_code": [2201, 9999]})

        with pytest.raises(ValueError, match="9999") as exc_info:
            convert_dac2b_to_dotstat_codes(df)

        assert str(MAPPINGS["dac2b_measure_dotstat"]) in str(exc_info.value)

    def test_mapping_matches_documented_conversion(self):
        """The mapping file itself carries the 13 documented API -> .stat codes."""
        mapping = dac2b_measure_mapping()

        assert mapping == {code: code - 2000 for code in ALL_API_CODES}
