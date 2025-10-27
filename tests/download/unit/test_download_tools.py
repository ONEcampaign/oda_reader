"""Unit tests for download tools with mocked API responses."""

import pytest

from oda_reader.common import get_data_from_api


@pytest.mark.unit
class TestDownloadWithMocks:
    """Test download functions with mocked HTTP responses."""

    def test_get_data_from_api_success(self, mocker, sample_csv_response):
        """Test successful API data retrieval."""
        # Mock the _get_response_text function
        mock_response = (200, sample_csv_response, False)
        mocker.patch(
            "oda_reader.common._get_response_text",
            return_value=mock_response,
        )

        result = get_data_from_api("https://example.com/data")

        assert result == sample_csv_response
        assert "DONOR,RECIPIENT" in result

    def test_get_data_from_api_404_triggers_retry(self, mocker):
        """Test that 404 with 'Dataflow' message triggers version fallback."""
        # First call returns 404 with Dataflow message
        # Second call (with fallback version) returns success
        mock_responses = [
            (404, "Dataflow not found", False),
            (200, "DONOR,VALUE\n1,100", False),
        ]

        mock = mocker.patch(
            "oda_reader.common._get_response_text",
            side_effect=mock_responses,
        )

        # URL with version 2.0 should fallback to 1.9
        url = "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DF_DAC1,2.0/"
        result = get_data_from_api(url)

        # Should have made 2 calls (original + fallback)
        assert mock.call_count == 2
        assert result == "DONOR,VALUE\n1,100"

    def test_get_data_from_api_non_404_error_raises(self, mocker):
        """Test that non-404 errors raise ConnectionError."""
        mock_response = (500, "Internal Server Error", False)
        mocker.patch(
            "oda_reader.common._get_response_text",
            return_value=mock_response,
        )

        with pytest.raises(ConnectionError, match="Error 500"):
            get_data_from_api("https://example.com/data")
