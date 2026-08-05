"""Unit tests for DAC1 bulk download functionality."""

import pandas as pd
import pytest

import oda_reader
from oda_reader.dac1 import (
    DAC1_BULK_LABEL,
    DAC1_FLOW_URL,
    bulk_download_dac1,
)

_RESOLVED_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC1/Table1_Data.zip"


@pytest.mark.unit
class TestDAC1BulkDownload:
    """Test DAC1 bulk download functions with mocked dependencies."""

    def test_bulk_download_dac1_is_exported(self):
        """bulk_download_dac1 is importable from the oda_reader top level."""
        assert oda_reader.bulk_download_dac1 is bulk_download_dac1
        assert "bulk_download_dac1" in oda_reader.__all__

    def test_bulk_download_dac1_returns_dataframe(self, mocker):
        """Test that bulk_download_dac1 returns DataFrame when no save path."""
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        mocker.patch(
            "oda_reader.dac1.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac1.bulk_download_parquet",
            return_value=mock_df,
        )

        result = bulk_download_dac1()

        assert result is mock_df
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=DAC1_FLOW_URL,
            label=DAC1_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac1_saves_to_path(self, mocker, tmp_path):
        """Test that bulk_download_dac1 passes save path to underlying function."""
        mocker.patch(
            "oda_reader.dac1.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac1.bulk_download_parquet",
            return_value=None,
        )

        result = bulk_download_dac1(save_to_path=tmp_path)

        assert result is None
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=tmp_path,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=DAC1_FLOW_URL,
            label=DAC1_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac1_as_iterator(self, mocker):
        """Test that bulk_download_dac1 passes as_iterator flag correctly."""

        def mock_iterator():
            yield pd.DataFrame({"col1": [1]})
            yield pd.DataFrame({"col1": [2]})

        mocker.patch(
            "oda_reader.dac1.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac1.bulk_download_parquet",
            return_value=mock_iterator(),
        )

        result = bulk_download_dac1(as_iterator=True)

        # Result should be an iterator
        assert hasattr(result, "__iter__")
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=True,
            use_raw_cache=True,
            flow_url=DAC1_FLOW_URL,
            label=DAC1_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac1_use_raw_cache_false(self, mocker):
        """Test that bulk_download_dac1 passes use_raw_cache through unchanged."""
        mocker.patch(
            "oda_reader.dac1.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac1.bulk_download_parquet",
            return_value=None,
        )

        bulk_download_dac1(use_raw_cache=False)

        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=False,
            flow_url=DAC1_FLOW_URL,
            label=DAC1_BULK_LABEL,
            version="etag-abc123",
        )
