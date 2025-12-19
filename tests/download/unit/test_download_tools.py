"""Unit tests for download tools with mocked API responses."""

import io
import warnings
import zipfile

import pandas as pd
import pytest

from oda_reader.common import get_data_from_api
from oda_reader.download.download_tools import (
    _detect_delimiter,
    _save_or_return_parquet_files_from_content,
    bulk_download_parquet,
)


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


@pytest.mark.unit
class TestDetectDelimiter:
    """Test delimiter detection for CSV/txt files."""

    def test_detect_comma_delimiter(self):
        """Test that comma-delimited content is detected correctly."""
        csv_content = "col1,col2,col3\nval1,val2,val3\nval4,val5,val6"
        file_obj = io.BytesIO(csv_content.encode("utf-8"))

        delimiter = _detect_delimiter(file_obj)

        assert delimiter == ","
        # Verify file position was reset
        assert file_obj.tell() == 0

    def test_detect_pipe_delimiter(self):
        """Test that pipe-delimited content is detected correctly."""
        csv_content = "col1|col2|col3\nval1|val2|val3\nval4|val5|val6"
        file_obj = io.BytesIO(csv_content.encode("utf-8"))

        delimiter = _detect_delimiter(file_obj)

        assert delimiter == "|"
        assert file_obj.tell() == 0

    def test_detect_tab_delimiter(self):
        """Test that tab-delimited content is detected correctly."""
        csv_content = "col1\tcol2\tcol3\nval1\tval2\tval3"
        file_obj = io.BytesIO(csv_content.encode("utf-8"))

        delimiter = _detect_delimiter(file_obj)

        assert delimiter == "\t"
        assert file_obj.tell() == 0

    def test_comma_wins_when_ambiguous(self):
        """Test that comma is preferred when sniffing fails and counts are equal."""
        # Content with no clear delimiter
        csv_content = "just some text without clear delimiters"
        file_obj = io.BytesIO(csv_content.encode("utf-8"))

        delimiter = _detect_delimiter(file_obj)

        # Should default to comma when counts are equal (both 0)
        assert delimiter == ","

    def test_works_with_string_io(self):
        """Test that delimiter detection works with StringIO objects too."""
        csv_content = "col1;col2;col3\nval1;val2;val3"
        file_obj = io.StringIO(csv_content)

        delimiter = _detect_delimiter(file_obj)

        assert delimiter == ";"
        assert file_obj.tell() == 0


@pytest.mark.unit
class TestFileTypeAutoDetection:
    """Test automatic file type detection in zip archives."""

    def _create_zip_with_parquet(self) -> bytes:
        """Create a zip file containing a parquet file."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_data.parquet", parquet_buffer.getvalue())
        return zip_buffer.getvalue()

    def _create_zip_with_txt(self, delimiter: str = ",") -> bytes:
        """Create a zip file containing a txt file."""
        if delimiter == "|":
            csv_content = "col1|col2|col3\n1|2|3\n4|5|6"
        else:
            csv_content = "col1,col2,col3\n1,2,3\n4,5,6"

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_data.txt", csv_content.encode("utf-8"))
        return zip_buffer.getvalue()

    def _create_zip_with_csv(self, delimiter: str = ",") -> bytes:
        """Create a zip file containing a .csv file."""
        if delimiter == "|":
            csv_content = "col1|col2|col3\n1|2|3\n4|5|6"
        else:
            csv_content = "col1,col2,col3\n1,2,3\n4,5,6"

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_data.csv", csv_content.encode("utf-8"))
        return zip_buffer.getvalue()

    def test_auto_detect_parquet_files(self):
        """Test that parquet files are auto-detected and read correctly."""
        zip_content = self._create_zip_with_parquet()

        result = _save_or_return_parquet_files_from_content(zip_content)

        assert result is not None
        assert len(result) == 1
        assert isinstance(result[0], pd.DataFrame)
        assert list(result[0].columns) == ["col1", "col2"]
        assert len(result[0]) == 3

    def test_auto_detect_txt_files_comma(self):
        """Test that comma-delimited txt files are auto-detected."""
        zip_content = self._create_zip_with_txt(delimiter=",")

        result = _save_or_return_parquet_files_from_content(zip_content)

        assert result is not None
        assert len(result) == 1
        assert isinstance(result[0], pd.DataFrame)
        assert list(result[0].columns) == ["col1", "col2", "col3"]

    def test_auto_detect_txt_files_pipe(self):
        """Test that pipe-delimited txt files are auto-detected."""
        zip_content = self._create_zip_with_txt(delimiter="|")

        result = _save_or_return_parquet_files_from_content(zip_content)

        assert result is not None
        assert len(result) == 1
        df = result[0]
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["col1", "col2", "col3"]
        assert len(df) == 2

    def test_auto_detect_csv_files(self):
        """Test that .csv files are auto-detected and read correctly."""
        zip_content = self._create_zip_with_csv(delimiter=",")

        result = _save_or_return_parquet_files_from_content(zip_content)

        assert result is not None
        assert len(result) == 1
        assert isinstance(result[0], pd.DataFrame)
        assert list(result[0].columns) == ["col1", "col2", "col3"]
        assert len(result[0]) == 2

    def test_auto_detect_csv_files_pipe(self):
        """Test that pipe-delimited .csv files are auto-detected."""
        zip_content = self._create_zip_with_csv(delimiter="|")

        result = _save_or_return_parquet_files_from_content(zip_content)

        assert result is not None
        assert len(result) == 1
        df = result[0]
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["col1", "col2", "col3"]
        assert len(df) == 2

    def test_save_csv_as_parquet_to_path(self, tmp_path):
        """Test that .csv files are converted to parquet when saving."""
        zip_content = self._create_zip_with_csv()

        result = _save_or_return_parquet_files_from_content(
            zip_content, save_to_path=tmp_path
        )

        assert result is None
        saved_files = list(tmp_path.glob("*.parquet"))
        assert len(saved_files) == 1
        # Verify conversion to parquet with correct name
        assert saved_files[0].suffix == ".parquet"
        assert "test_data" in saved_files[0].name
        df = pd.read_parquet(saved_files[0])
        assert len(df) == 2

    def test_save_parquet_to_path(self, tmp_path):
        """Test saving parquet files to a path."""
        zip_content = self._create_zip_with_parquet()

        result = _save_or_return_parquet_files_from_content(
            zip_content, save_to_path=tmp_path
        )

        assert result is None
        saved_files = list(tmp_path.glob("*.parquet"))
        assert len(saved_files) == 1
        # Verify the saved file can be read
        df = pd.read_parquet(saved_files[0])
        assert len(df) == 3

    def test_save_txt_as_parquet_to_path(self, tmp_path):
        """Test that txt files are converted to parquet when saving."""
        zip_content = self._create_zip_with_txt()

        result = _save_or_return_parquet_files_from_content(
            zip_content, save_to_path=tmp_path
        )

        assert result is None
        saved_files = list(tmp_path.glob("*.parquet"))
        assert len(saved_files) == 1
        # Verify conversion to parquet
        assert saved_files[0].suffix == ".parquet"
        df = pd.read_parquet(saved_files[0])
        assert len(df) == 2

    def test_raises_on_empty_zip(self):
        """Test that ValueError is raised when zip has no valid files."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("readme.md", "Not a data file")

        with pytest.raises(ValueError, match="No parquet, csv, or txt files"):
            _save_or_return_parquet_files_from_content(zip_buffer.getvalue())

    def test_txt_iterator_raises(self):
        """Test that as_iterator raises for txt files."""
        zip_content = self._create_zip_with_txt()

        with pytest.raises(ValueError, match="Streaming not supported"):
            _save_or_return_parquet_files_from_content(zip_content, as_iterator=True)


@pytest.mark.unit
class TestDeprecationWarnings:
    """Test deprecation warnings for backward compatibility."""

    def test_is_txt_parameter_emits_deprecation_warning(self, mocker):
        """Test that using is_txt parameter emits a deprecation warning."""
        # Mock the internal functions to avoid actual downloads
        mocker.patch(
            "oda_reader.download.download_tools._get_temp_file",
            return_value=("/fake/path", False),
        )
        mocker.patch(
            "oda_reader.download.download_tools._save_or_return_parquet_files_from_content",
            return_value=[pd.DataFrame({"col": [1, 2]})],
        )

        with pytest.warns(DeprecationWarning, match="is_txt.*deprecated"):
            bulk_download_parquet("fake-id", is_txt=True)

    def test_is_txt_false_also_emits_warning(self, mocker):
        """Test that is_txt=False also emits deprecation warning."""
        mocker.patch(
            "oda_reader.download.download_tools._get_temp_file",
            return_value=("/fake/path", False),
        )
        mocker.patch(
            "oda_reader.download.download_tools._save_or_return_parquet_files_from_content",
            return_value=[pd.DataFrame({"col": [1, 2]})],
        )

        with pytest.warns(DeprecationWarning, match="is_txt.*deprecated"):
            bulk_download_parquet("fake-id", is_txt=False)

    def test_no_warning_when_is_txt_not_provided(self, mocker):
        """Test that no warning is emitted when is_txt is not provided."""
        mocker.patch(
            "oda_reader.download.download_tools._get_temp_file",
            return_value=("/fake/path", False),
        )
        mocker.patch(
            "oda_reader.download.download_tools._save_or_return_parquet_files_from_content",
            return_value=[pd.DataFrame({"col": [1, 2]})],
        )

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            # Should not raise any DeprecationWarning
            bulk_download_parquet("fake-id")
