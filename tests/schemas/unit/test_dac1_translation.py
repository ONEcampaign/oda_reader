"""Unit tests for DAC1 schema translation."""

import pytest

from oda_reader.schemas.dac1_translation import area_code_mapping


@pytest.mark.unit
class TestDAC1Translation:
    """Test DAC1 code translations."""

    def test_area_code_mapping_returns_dict(self):
        """Test that area_code_mapping returns a dictionary."""
        result = area_code_mapping()

        assert isinstance(result, dict)
        assert len(result) > 0

    def test_area_code_mapping_has_expected_size(self):
        """Test that area_code_mapping returns expected number of mappings."""
        result = area_code_mapping()

        # As of 2025, there should be hundreds of area codes
        assert len(result) > 400, f"Expected >400 mappings, got {len(result)}"

    @pytest.mark.parametrize(
        "new_code,expected_old_code",
        [
            # DAC members (donors)
            (801, "AUS"),  # Australia
            (1, "AUT"),  # Austria
            (2, "BEL"),  # Belgium
            (301, "CAN"),  # Canada
            (3, "DNK"),  # Denmark
            (4, "FRA"),  # France
            (5, "DEU"),  # Germany
            (18, "FIN"),  # Finland
            (40, "GRC"),  # Greece
            (75, "HUN"),  # Hungary
            (742, "KOR"),  # Korea
            (22, "LUX"),  # Luxembourg
            (358, "MEX"),  # Mexico
            # Additional codes
            (434, "CHL"),  # Chile
            (437, "COL"),  # Colombia
            (336, "CRI"),  # Costa Rica
            (68, "CZE"),  # Czech Republic
            (82, "EST"),  # Estonia
            (83, "LVA"),  # Latvia
            (84, "LTU"),  # Lithuania
        ],
    )
    def test_donor_code_translation_spot_check(self, new_code, expected_old_code):
        """Spot check critical donor code translations."""
        mapping = area_code_mapping()

        assert new_code in mapping, f"Code {new_code} not found in mapping"
        assert mapping[new_code] == expected_old_code, (
            f"Expected {new_code} -> {expected_old_code}, "
            f"got {new_code} -> {mapping[new_code]}"
        )

    def test_area_code_mapping_keys_are_integers(self):
        """Test that area_code_mapping keys are integers."""
        result = area_code_mapping()

        for key in result:
            assert isinstance(key, int), f"Key {key} is not an integer, got {type(key)}"

    def test_area_code_mapping_values_are_strings(self):
        """Test that area_code_mapping values are strings."""
        result = area_code_mapping()

        for value in result.values():
            assert isinstance(
                value, str
            ), f"Value {value} is not a string, got {type(value)}"

    def test_area_code_mapping_no_empty_values(self):
        """Test that area_code_mapping has no empty values."""
        result = area_code_mapping()

        for key, value in result.items():
            assert value, f"Key {key} has empty value"
            assert len(value) > 0, f"Key {key} has empty string value"
