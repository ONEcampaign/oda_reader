"""Unit tests for download tools with mocked API responses."""

import io
import warnings
import zipfile

import pandas as pd
import pytest

from oda_reader.common import get_data_from_api
from oda_reader.download.download_tools import (
    _detect_delimiter,
    _extract_dataflow_id_from_flow_url,
    _save_or_return_parquet_files_from_content,
    bulk_download_parquet,
    get_bulk_file_id,
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

    def test_get_data_from_api_404_triggers_version_discovery(self, mocker):
        """Test that a 'Dataflow not found' response triggers version discovery retry."""
        mock_get_response = mocker.patch(
            "oda_reader.common._get_response_text",
            side_effect=[
                (404, "Dataflow not found", False),
                (200, "DONOR,VALUE\n1,100", False),
            ],
        )
        mocker.patch(
            "oda_reader.common.discover_latest_version",
            return_value="1.9",
        )
        mocker.patch(
            "oda_reader.common.get_dimension_count",
            return_value=7,
        )

        url = (
            "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,2.0/"
        )
        result = get_data_from_api(url)

        assert mock_get_response.call_count == 2
        assert result == "DONOR,VALUE\n1,100"

    def test_get_data_from_api_discovered_version_matches_raises(self, mocker):
        """Test that matching discovered version raises immediately without retry."""
        mocker.patch(
            "oda_reader.common._get_response_text",
            return_value=(404, "Dataflow not found", False),
        )
        mocker.patch(
            "oda_reader.common.discover_latest_version",
            return_value="2.0",  # same as URL version
        )

        url = (
            "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,2.0/"
        )
        with pytest.raises(ConnectionError, match="matches the attempted version"):
            get_data_from_api(url)

    def test_get_data_from_api_incompatible_dsd_raises(self, mocker):
        """Test that a discovered version with different dimension count raises."""
        mocker.patch(
            "oda_reader.common._get_response_text",
            side_effect=[
                (404, "Dataflow not found", False),
                (200, "DONOR,VALUE\n1,100", False),
            ],
        )
        mocker.patch(
            "oda_reader.common.discover_latest_version",
            return_value="3.0",
        )
        mocker.patch(
            "oda_reader.common.get_dimension_count",
            side_effect=[7, 8],  # old has 7, new has 8 — breaking change
        )

        url = (
            "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,2.0/"
        )
        with pytest.raises(ConnectionError, match="breaking schema change"):
            get_data_from_api(url)

    def test_get_data_from_api_compatible_upgrade_succeeds(self, mocker):
        """Test that auto-upgrade works when dimension count matches."""
        mocker.patch(
            "oda_reader.common._get_response_text",
            side_effect=[
                (404, "Dataflow not found", False),
                (200, "DONOR,VALUE\n1,100", False),
            ],
        )
        mocker.patch(
            "oda_reader.common.discover_latest_version",
            return_value="3.0",
        )
        mocker.patch(
            "oda_reader.common.get_dimension_count",
            return_value=7,  # same count — compatible
        )

        url = (
            "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,2.0/"
        )
        result = get_data_from_api(url)
        assert result == "DONOR,VALUE\n1,100"

    def test_get_data_from_api_dsd_check_fails_gracefully(self, mocker):
        """Test that DSD check failure doesn't block the retry."""
        mocker.patch(
            "oda_reader.common._get_response_text",
            side_effect=[
                (404, "Dataflow not found", False),
                (200, "DONOR,VALUE\n1,100", False),
            ],
        )
        mocker.patch(
            "oda_reader.common.discover_latest_version",
            return_value="1.9",
        )
        mocker.patch(
            "oda_reader.common.get_dimension_count",
            side_effect=ConnectionError("DSD endpoint down"),
        )

        url = (
            "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,2.0/"
        )
        result = get_data_from_api(url)
        assert result == "DONOR,VALUE\n1,100"

    def test_get_data_from_api_retry_also_fails_raises(self, mocker):
        """Test that failed retry after discovery raises clearly."""
        mocker.patch(
            "oda_reader.common._get_response_text",
            return_value=(404, "Dataflow not found", False),
        )
        mocker.patch(
            "oda_reader.common.discover_latest_version",
            return_value="1.9",
        )
        mocker.patch(
            "oda_reader.common.get_dimension_count",
            return_value=7,
        )

        url = (
            "https://sdmx.oecd.org/public/rest/data/OECD.DCD.FSD,DSD_DAC1@DF_DAC1,2.0/"
        )
        with pytest.raises(ConnectionError, match="even after version discovery"):
            get_data_from_api(url)

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

    @staticmethod
    def _mock_download_pipeline(mocker):
        """Stub the cache manager + content extractor to avoid real downloads."""
        fake_manager = mocker.Mock()
        fake_manager.ensure.return_value = "/fake/path"
        mocker.patch(
            "oda_reader.download.download_tools.bulk_cache_manager",
            return_value=fake_manager,
        )
        mocker.patch(
            "oda_reader.download.download_tools._save_or_return_parquet_files_from_content",
            return_value=[pd.DataFrame({"col": [1, 2]})],
        )

    def test_is_txt_parameter_emits_deprecation_warning(self, mocker):
        """Test that using is_txt parameter emits a deprecation warning."""
        self._mock_download_pipeline(mocker)

        with pytest.warns(DeprecationWarning, match="is_txt.*deprecated"):
            bulk_download_parquet("fake-id", is_txt=True)

    def test_is_txt_false_also_emits_warning(self, mocker):
        """Test that is_txt=False also emits deprecation warning."""
        self._mock_download_pipeline(mocker)

        with pytest.warns(DeprecationWarning, match="is_txt.*deprecated"):
            bulk_download_parquet("fake-id", is_txt=False)

    def test_no_warning_when_is_txt_not_provided(self, mocker):
        """Test that no warning is emitted when is_txt is not provided."""
        self._mock_download_pipeline(mocker)

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            bulk_download_parquet("fake-id")


@pytest.mark.unit
class TestExtractDataflowIdFromFlowUrl:
    """Test the helper that extracts a dataflow ID from a bulk-download flow URL."""

    @pytest.mark.parametrize(
        "url,expected",
        [
            (
                "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/",
                "DSD_CRS@DF_CRS",
            ),
            (
                "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_DAC2@DF_DAC2A/",
                "DSD_DAC2@DF_DAC2A",
            ),
            (
                "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_MULTI@DF_MULTI/",
                "DSD_MULTI@DF_MULTI",
            ),
            # trailing slash optional
            (
                "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS",
                "DSD_CRS@DF_CRS",
            ),
        ],
    )
    def test_recognized_urls(self, url, expected):
        assert _extract_dataflow_id_from_flow_url(url) == expected

    def test_unrecognized_url_returns_none(self):
        assert (
            _extract_dataflow_id_from_flow_url("https://example.com/other/path") is None
        )


FLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/"
SEARCH_STRING = "DF_CRS_BULK="


@pytest.mark.unit
class TestGetBulkFileId:
    """Test get_bulk_file_id with discovery and fallback paths."""

    def test_explicit_version_succeeds_immediately(self, mocker):
        """When latest_flow is provided and works, discovery is not called."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(200, f"{SEARCH_STRING}abc123</end>", False),
        )
        mock_discover = mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
        )

        result = get_bulk_file_id(FLOW_URL, SEARCH_STRING, latest_flow=1.6)

        assert result == "abc123"
        mock_discover.assert_not_called()

    def test_discovery_succeeds_when_no_explicit_version(self, mocker):
        """When latest_flow=None, discovery is used and succeeds."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(200, f"{SEARCH_STRING}xyz789</end>", False),
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            return_value="1.7",
        )

        result = get_bulk_file_id(FLOW_URL, SEARCH_STRING)
        assert result == "xyz789"

    def test_explicit_version_fails_then_discovery_rescues(self, mocker):
        """When explicit version fails, discovery finds a working version."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            side_effect=[
                (404, "Not found", False),  # explicit version fails
                (
                    200,
                    f"{SEARCH_STRING}rescued</end>",
                    False,
                ),  # discovered version works
            ],
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            return_value="1.7",
        )

        result = get_bulk_file_id(FLOW_URL, SEARCH_STRING, latest_flow=1.8)
        assert result == "rescued"

    def test_discovery_fails_then_scan_rescues(self, mocker):
        """When discovery raises, the decrement scan finds a working version."""
        responses = [(404, "Not found", False)] * 5 + [
            (200, f"{SEARCH_STRING}scanned</end>", False),
        ]
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            side_effect=responses,
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            side_effect=ConnectionError("metadata endpoint down"),
        )

        result = get_bulk_file_id(FLOW_URL, SEARCH_STRING, latest_flow=2.0)
        assert result == "scanned"

    def test_all_methods_exhausted_raises(self, mocker):
        """When discovery and scan both fail, RuntimeError is raised."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(404, "Not found", False),
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            side_effect=ConnectionError("metadata endpoint down"),
        )

        with pytest.raises(RuntimeError, match="could not be found"):
            get_bulk_file_id(FLOW_URL, SEARCH_STRING, latest_flow=1.0)
