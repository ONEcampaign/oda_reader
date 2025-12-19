"""Unit tests for DAC2a bulk download functionality."""

import pytest

from oda_reader.dac2a import bulk_download_dac2a, get_full_dac2a_parquet_id


@pytest.mark.unit
class TestDAC2aBulkDownload:
    """Test DAC2a bulk download functions with mocked dependencies."""

    def test_get_full_dac2a_parquet_id_calls_correct_function(self, mocker):
        """Test that get_full_dac2a_parquet_id calls get_bulk_file_id with correct params."""
        mock_get_bulk = mocker.patch(
            "oda_reader.dac2a.get_bulk_file_id",
            return_value="test-file-id-123",
        )

        result = get_full_dac2a_parquet_id()

        assert result == "test-file-id-123"
        mock_get_bulk.assert_called_once()
        # Verify the flow URL contains DAC2A
        call_kwargs = mock_get_bulk.call_args.kwargs
        assert "DAC2A" in call_kwargs["flow_url"]
        # Verify the search string is for DAC2A full dataset
        assert "DAC2A full dataset" in call_kwargs["search_string"]

    def test_bulk_download_dac2a_returns_dataframe(self, mocker):
        """Test that bulk_download_dac2a returns DataFrame when no save path."""
        import pandas as pd

        mock_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

        mocker.patch(
            "oda_reader.dac2a.get_full_dac2a_parquet_id",
            return_value="mock-file-id",
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2a.bulk_download_parquet",
            return_value=mock_df,
        )

        result = bulk_download_dac2a()

        assert result is mock_df
        mock_bulk.assert_called_once_with(
            file_id="mock-file-id",
            save_to_path=None,
            as_iterator=False,
        )

    def test_bulk_download_dac2a_saves_to_path(self, mocker, tmp_path):
        """Test that bulk_download_dac2a passes save path to underlying function."""
        mocker.patch(
            "oda_reader.dac2a.get_full_dac2a_parquet_id",
            return_value="mock-file-id",
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2a.bulk_download_parquet",
            return_value=None,
        )

        result = bulk_download_dac2a(save_to_path=tmp_path)

        assert result is None
        mock_bulk.assert_called_once_with(
            file_id="mock-file-id",
            save_to_path=tmp_path,
            as_iterator=False,
        )

    def test_bulk_download_dac2a_as_iterator(self, mocker):
        """Test that bulk_download_dac2a passes as_iterator flag correctly."""
        import pandas as pd

        def mock_iterator():
            yield pd.DataFrame({"col1": [1]})
            yield pd.DataFrame({"col1": [2]})

        mocker.patch(
            "oda_reader.dac2a.get_full_dac2a_parquet_id",
            return_value="mock-file-id",
        )
        mock_bulk = mocker.patch(
            "oda_reader.dac2a.bulk_download_parquet",
            return_value=mock_iterator(),
        )

        result = bulk_download_dac2a(as_iterator=True)

        # Result should be an iterator
        assert hasattr(result, "__iter__")
        mock_bulk.assert_called_once_with(
            file_id="mock-file-id",
            save_to_path=None,
            as_iterator=True,
        )
