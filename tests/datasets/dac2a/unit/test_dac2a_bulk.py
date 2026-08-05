"""Unit tests for DAC2a bulk download functionality."""

import pandas as pd
import pytest

from oda_reader.dac2a import (
    DAC2A_BULK_LABEL,
    DAC2A_FLOW_URL,
    bulk_download_dac2a,
    get_full_dac2a_parquet_id,
    get_full_dac2a_parquet_url,
)

_RESOLVED_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip"


@pytest.mark.unit
class TestDAC2aBulkDownload:
    """Test DAC2a bulk download functions with mocked dependencies."""

    def test_get_full_dac2a_parquet_url_calls_correct_function(self, mocker):
        """get_full_dac2a_parquet_url calls get_bulk_file_url with correct params."""
        mock_get_bulk = mocker.patch(
            "oda_reader.dac2a.get_bulk_file_url",
            return_value=_RESOLVED_URL,
        )

        result = get_full_dac2a_parquet_url()

        assert result == _RESOLVED_URL
        mock_get_bulk.assert_called_once_with(
            flow_url=DAC2A_FLOW_URL, label=DAC2A_BULK_LABEL
        )

    def test_get_full_dac2a_parquet_id_is_deprecated_and_delegates(self, mocker):
        """The old *_id name still works but warns and now returns a URL."""
        mocker.patch(
            "oda_reader.dac2a.get_bulk_file_url",
            return_value=_RESOLVED_URL,
        )

        with pytest.warns(DeprecationWarning, match="get_full_dac2a_parquet_url"):
            result = get_full_dac2a_parquet_id()

        assert result == _RESOLVED_URL

    def test_bulk_download_dac2a_returns_dataframe(self, mocker):
        """Test that bulk_download_dac2a returns DataFrame when no save path."""
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        mocker.patch(
            "oda_reader.dac2a.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2a.bulk_download_parquet",
            return_value=mock_df,
        )

        result = bulk_download_dac2a()

        assert result is mock_df
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=DAC2A_FLOW_URL,
            label=DAC2A_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac2a_saves_to_path(self, mocker, tmp_path):
        """Test that bulk_download_dac2a passes save path to underlying function."""
        mocker.patch(
            "oda_reader.dac2a.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2a.bulk_download_parquet",
            return_value=None,
        )

        result = bulk_download_dac2a(save_to_path=tmp_path)

        assert result is None
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=tmp_path,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=DAC2A_FLOW_URL,
            label=DAC2A_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac2a_as_iterator(self, mocker):
        """Test that bulk_download_dac2a passes as_iterator flag correctly."""

        def mock_iterator():
            yield pd.DataFrame({"col1": [1]})
            yield pd.DataFrame({"col1": [2]})

        mocker.patch(
            "oda_reader.dac2a.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2a.bulk_download_parquet",
            return_value=mock_iterator(),
        )

        result = bulk_download_dac2a(as_iterator=True)

        # Result should be an iterator
        assert hasattr(result, "__iter__")
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=True,
            use_raw_cache=True,
            flow_url=DAC2A_FLOW_URL,
            label=DAC2A_BULK_LABEL,
            version="etag-abc123",
        )
