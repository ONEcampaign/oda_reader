"""Unit tests for DAC2b bulk download functionality."""

import pandas as pd
import pytest

from oda_reader.dac2b import (
    DAC2B_BULK_LABEL,
    DAC2B_FLOW_URL,
    bulk_download_dac2b,
)

_RESOLVED_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2b_Data.zip"


@pytest.mark.unit
class TestDAC2bBulkDownload:
    """Test DAC2b bulk download functions with mocked dependencies."""

    def test_bulk_download_dac2b_returns_dataframe(self, mocker):
        """Test that bulk_download_dac2b returns DataFrame when no save path."""
        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        mocker.patch(
            "oda_reader.dac2b.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2b.bulk_download_parquet",
            return_value=mock_df,
        )

        result = bulk_download_dac2b()

        assert result is mock_df
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=DAC2B_FLOW_URL,
            label=DAC2B_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac2b_saves_to_path(self, mocker, tmp_path):
        """Test that bulk_download_dac2b passes save path to underlying function."""
        mocker.patch(
            "oda_reader.dac2b.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2b.bulk_download_parquet",
            return_value=None,
        )

        result = bulk_download_dac2b(save_to_path=tmp_path)

        assert result is None
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=tmp_path,
            as_iterator=False,
            use_raw_cache=True,
            flow_url=DAC2B_FLOW_URL,
            label=DAC2B_BULK_LABEL,
            version="etag-abc123",
        )

    def test_bulk_download_dac2b_as_iterator(self, mocker):
        """Test that bulk_download_dac2b passes as_iterator flag correctly."""

        def mock_iterator():
            yield pd.DataFrame({"col1": [1]})
            yield pd.DataFrame({"col1": [2]})

        mocker.patch(
            "oda_reader.dac2b.get_bulk_file_url_with_version",
            return_value=(_RESOLVED_URL, "etag-abc123"),
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2b.bulk_download_parquet",
            return_value=mock_iterator(),
        )

        result = bulk_download_dac2b(as_iterator=True)

        # Result should be an iterator
        assert hasattr(result, "__iter__")
        mock_bulk.assert_called_once_with(
            url=_RESOLVED_URL,
            save_to_path=None,
            as_iterator=True,
            use_raw_cache=True,
            flow_url=DAC2B_FLOW_URL,
            label=DAC2B_BULK_LABEL,
            version="etag-abc123",
        )
