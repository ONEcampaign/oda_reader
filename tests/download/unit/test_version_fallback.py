"""Unit tests for dataflow version fallback logic."""

import pytest

from oda_reader.common import _get_dataflow_version, _replace_dataflow_version


@pytest.mark.unit
class TestVersionFallback:
    """Test dataflow version manipulation."""

    @pytest.mark.parametrize(
        "url,expected_version",
        [
            (
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,1.0/",
                "1.0",
            ),
            (
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC2A,2.5/",
                "2.5",
            ),
            (
                "https://sdmx.oecd.org/dcd-public/rest/data/OECD.DCD.FSD,DF_CRS,3.2/",
                "3.2",
            ),
        ],
    )
    def test_get_dataflow_version(self, url, expected_version):
        """Test extracting version from URL."""
        result = _get_dataflow_version(url)
        assert result == expected_version

    @pytest.mark.parametrize(
        "original_url,new_version,expected_url",
        [
            (
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,2.0/",
                "1.9",
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,1.9/",
            ),
            (
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,2.0/filters",
                "1.5",
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,1.5/filters",
            ),
        ],
    )
    def test_replace_dataflow_version(self, original_url, new_version, expected_url):
        """Test replacing version in URL."""
        result = _replace_dataflow_version(original_url, new_version)
        assert result == expected_url
