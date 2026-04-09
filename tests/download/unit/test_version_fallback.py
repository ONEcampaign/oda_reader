"""Unit tests for dataflow version fallback logic."""

import pytest

from oda_reader.common import (
    _extract_dataflow_id,
    _get_dataflow_version,
    _replace_dataflow_version,
)


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
            # v2 slash-separated
            (
                "https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DF_DAC1/1.8/*",
                "1.8",
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
            # v2 slash-separated
            (
                "https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DF_DAC1/2.0/*",
                "1.8",
                "https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DF_DAC1/1.8/*",
            ),
        ],
    )
    def test_replace_dataflow_version(self, original_url, new_version, expected_url):
        """Test replacing version in URL."""
        result = _replace_dataflow_version(original_url, new_version)
        assert result == expected_url


@pytest.mark.unit
class TestExtractDataflowId:
    """Test extracting dataflow ID from SDMX data URLs."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            # v1 comma-separated
            (
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,1.8/all",
                "DSD_DAC1@DF_DAC1",
            ),
            # v2 slash-separated
            (
                "https://sdmx.oecd.org/public/rest/v2/data/dataflow/OECD.DCD.FSD/DSD_DAC1@DF_DAC1/1.8/*",
                "DSD_DAC1@DF_DAC1",
            ),
            # CRS URL (dcd-public prefix, v1)
            (
                "https://sdmx.oecd.org/dcd-public/rest/data/OECD.DCD.FSD,DSD_CRS@DF_CRS,1.6/all",
                "DSD_CRS@DF_CRS",
            ),
            # DAC2A v1
            (
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC2@DF_DAC2A,1.4/all",
                "DSD_DAC2@DF_DAC2A",
            ),
        ],
    )
    def test_extract_dataflow_id(self, url, expected):
        """Test extracting dataflow ID from various URL patterns."""
        assert _extract_dataflow_id(url) == expected

    def test_unrecognized_url_returns_none(self):
        """URLs without a recognisable OECD.DCD.FSD pattern return None."""
        assert _extract_dataflow_id("https://example.com/data/something") is None

    def test_url_without_version_returns_none(self):
        """URLs without a version number return None."""
        assert (
            _extract_dataflow_id(
                "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1"
            )
            is None
        )
