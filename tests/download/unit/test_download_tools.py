"""Unit tests for download tools with mocked API responses."""

import io
import warnings
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import pytest
import requests

from oda_reader._cache.manager import CacheManager
from oda_reader.common import get_data_from_api
from oda_reader.download.download_tools import (
    _atomic_write,
    _check_truncated,
    _detect_delimiter,
    _extract_dataflow_id_from_flow_url,
    _fetch_revalidation_token,
    _finalize_resolved_url,
    _get_with_validated_redirects,
    _iter_csv_chunks,
    _parse_ext_resources,
    _safe_member_path,
    _save_or_return_parquet_files_from_content,
    _stream_to_file,
    bulk_download_parquet,
    get_bulk_file_id,
    get_bulk_file_url,
)
from oda_reader.exceptions import BulkDownloadHTTPError, BulkPayloadCorruptError

_FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "dataflow"


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

    def test_txt_iterator_yields_chunks(self):
        """as_iterator=True works for txt files."""
        zip_content = self._create_zip_with_txt()

        result = _save_or_return_parquet_files_from_content(
            zip_content, as_iterator=True
        )

        chunks = list(result)
        assert len(chunks) == 1
        assert isinstance(chunks[0], pd.DataFrame)
        assert list(chunks[0].columns) == ["col1", "col2", "col3"]
        assert len(chunks[0]) == 2

    def test_csv_iterator_yields_chunks(self):
        """as_iterator also works for .csv files, not just .txt."""
        zip_content = self._create_zip_with_csv()

        result = _save_or_return_parquet_files_from_content(
            zip_content, as_iterator=True
        )

        chunks = list(result)
        assert len(chunks) == 1
        assert isinstance(chunks[0], pd.DataFrame)
        assert list(chunks[0].columns) == ["col1", "col2", "col3"]

    def test_csv_iterator_respects_chunk_size(self):
        """Multiple chunks are yielded when the data exceeds chunk_size."""
        rows = "\n".join(f"{i},{i * 2},{i * 3}" for i in range(10))
        csv_content = f"col1,col2,col3\n{rows}\n"
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_data.csv", csv_content.encode("utf-8"))

        chunks = list(_iter_csv_chunks(zip_buffer.getvalue(), chunk_size=3))

        assert len(chunks) == 4  # 10 rows / chunk_size=3 -> 3+3+3+1
        combined = pd.concat(chunks, ignore_index=True)
        assert len(combined) == 10
        assert list(combined.columns) == ["col1", "col2", "col3"]
        assert combined["col1"].tolist() == list(range(10))

    def test_csv_iterator_matches_whole_file_dtypes_exactly(self):
        """Concatenated chunks must equal the non-iterator read exactly.

        Regression test for a real bug reproduced against the actual 2024
        CRS file: `pd.read_csv(..., chunksize=N)` infers dtypes
        independently per chunk. `Interest1` there mixes numeric-looking
        values (some with a leading zero, e.g. "04216") with genuinely
        non-numeric ones (interest-rate formulas like "EURIBOR6M+1.60%"),
        so the whole-file read correctly keeps it as strings -- but a chunk
        containing only the numeric-looking rows gets inferred as float in
        isolation, silently dropping the leading zero from "04216" before
        concatenation ever happens. `_iter_csv_chunks` must not exhibit
        this: it reads the member once and slices an already-parsed frame,
        so there is only ever one parse to disagree with itself.
        """
        # Row 2 ("04216") and row 4 ("EURIBOR...") land in different
        # 3-row chunks below, matching the real split that triggered this.
        rows = [
            "code|interest",
            "1|0",
            "2|04216",
            "3|1000",
            "4|EURIBOR6M+1.60%",
            "5|500",
        ]
        csv_content = "\n".join(rows) + "\n"
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as z:
            z.writestr("test_data.txt", csv_content.encode("utf-8"))

        whole = _save_or_return_parquet_files_from_content(zip_buffer.getvalue())[0]
        chunks = list(_iter_csv_chunks(zip_buffer.getvalue(), chunk_size=3))
        chunked = pd.concat(chunks, ignore_index=True)

        assert whole.equals(chunked)
        # The leading zero must actually survive in both -- confirms this
        # test would have caught the bug (a naive chunksize-based
        # implementation fails this exact assertion), not just passed
        # vacuously.
        assert "04216" in whole["interest"].tolist()
        assert "04216" in chunked["interest"].tolist()


@pytest.mark.unit
class TestAtomicWrite:
    """`_atomic_write`: tmp-sibling-then-replace, matching CacheManager's pattern."""

    def test_successful_write_lands_at_dest(self, tmp_path):
        dest = tmp_path / "out.bin"

        _atomic_write(dest, lambda tmp: tmp.write_bytes(b"hello"))

        assert dest.read_bytes() == b"hello"
        assert list(tmp_path.glob("*.tmp-*")) == []

    def test_failed_write_leaves_dest_untouched_and_cleans_tmp(self, tmp_path):
        """A disk-full or interrupted write must not leave a partial file at
        `dest`, and must not leave the tmp sibling behind either."""
        dest = tmp_path / "out.bin"
        dest.write_bytes(b"original good content")

        def _boom(tmp: Path) -> None:
            tmp.write_bytes(b"partial")
            raise OSError("simulated disk full")

        with pytest.raises(OSError, match="simulated disk full"):
            _atomic_write(dest, _boom)

        # The pre-existing good file at dest must survive untouched -- the
        # whole point of tmp-then-replace is that a failed write never
        # gets the chance to clobber it.
        assert dest.read_bytes() == b"original good content"
        assert list(tmp_path.glob("*.tmp-*")) == []

    def test_write_to_new_dest_leaves_nothing_on_failure(self, tmp_path):
        dest = tmp_path / "out.bin"

        def _boom(tmp: Path) -> None:
            tmp.write_bytes(b"partial")
            raise OSError("simulated disk full")

        with pytest.raises(OSError):
            _atomic_write(dest, _boom)

        assert not dest.exists()
        assert list(tmp_path.glob("*.tmp-*")) == []


@pytest.mark.unit
class TestSafeMemberPath:
    """`_safe_member_path`: the sanitizer behind the zip-slip fix.

    Unlike its basename-flattening predecessor (`_safe_member_name`), this
    preserves directory structure for legitimate nested members while still
    rejecting anything that could escape `dest_dir`.
    """

    def test_plain_name_resolves_inside_dest(self, tmp_path):
        result = _safe_member_path("CRS 2024 data.txt", tmp_path)
        assert result == (tmp_path / "CRS 2024 data.txt").resolve()

    def test_nested_structure_is_preserved(self, tmp_path):
        """Nested members keep their directory structure, not flattened to a basename."""
        result = _safe_member_path("a/b/data.parquet", tmp_path)
        assert result == (tmp_path / "a" / "b" / "data.parquet").resolve()

    def test_traversal_component_is_unsafe(self, tmp_path):
        assert _safe_member_path("../../../tmp/evil.parquet", tmp_path) is None
        assert _safe_member_path("a/../../etc/evil.parquet", tmp_path) is None

    def test_absolute_member_is_unsafe(self, tmp_path):
        assert _safe_member_path("/etc/cron.d/evil", tmp_path) is None

    def test_empty_name_is_unsafe(self, tmp_path):
        assert _safe_member_path("", tmp_path) is None

    def test_bare_dot_and_dotdot_are_unsafe(self, tmp_path):
        assert _safe_member_path(".", tmp_path) is None
        assert _safe_member_path("..", tmp_path) is None

    def test_embedded_dot_component_is_harmless(self, tmp_path):
        """`PurePosixPath` normalizes away a `.` segment on its own --
        `a/./b.parquet` is equivalent to `a/b.parquet`, not unsafe."""
        assert _safe_member_path("a/./b.parquet", tmp_path) == _safe_member_path(
            "a/b.parquet", tmp_path
        )


@pytest.mark.unit
class TestZipSlipSanitization:
    """A malicious zip member name must never escape `save_to_path`.

    Covers two exploit shapes: `../`-traversal, and an outright absolute
    member name (`Path(dest) / "/etc/x"` discards `dest` entirely, since
    pathlib evaluates the right operand's absoluteness first -- no `..`
    needed for that one). Both are *rejected* outright (the member is
    skipped, with a warning) rather than reinterpreted -- unlike the
    predecessor fix, which flattened a traversal/absolute member to its
    basename and wrote it inside `dest` anyway. Rejecting is what makes
    directory-structure preservation (below) safe: there's no silent
    reinterpretation step left that a crafted name could exploit.
    """

    def _malicious_zip(self, *, member_name: str, content: bytes) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            z.writestr(member_name, content)
        return buffer.getvalue()

    def test_traversal_parquet_member_is_skipped_entirely(self, tmp_path):
        dest = tmp_path / "dest"
        outside_marker = tmp_path / "escaped.parquet"
        df = pd.DataFrame({"a": [1]})
        buf = io.BytesIO()
        df.to_parquet(buf)
        zip_content = self._malicious_zip(
            member_name="../escaped.parquet", content=buf.getvalue()
        )

        _save_or_return_parquet_files_from_content(zip_content, save_to_path=dest)

        assert not outside_marker.exists()
        assert list(dest.glob("**/*")) == []

    def test_absolute_parquet_member_is_skipped_entirely(self, tmp_path):
        dest = tmp_path / "dest"
        df = pd.DataFrame({"a": [1]})
        buf = io.BytesIO()
        df.to_parquet(buf)
        zip_content = self._malicious_zip(
            member_name="/etc/cron.d/evil.parquet", content=buf.getvalue()
        )

        _save_or_return_parquet_files_from_content(zip_content, save_to_path=dest)

        assert not Path("/etc/cron.d/evil.parquet").exists()
        assert list(dest.glob("**/*")) == []

    def test_traversal_txt_member_is_skipped_entirely(self, tmp_path):
        dest = tmp_path / "dest"
        outside_marker = tmp_path / "escaped.parquet"
        zip_content = self._malicious_zip(
            member_name="../../escaped.txt", content=b"col1,col2\n1,2\n"
        )

        _save_or_return_parquet_files_from_content(zip_content, save_to_path=dest)

        assert not outside_marker.exists()
        assert list(dest.glob("**/*")) == []

    def test_absolute_txt_member_is_skipped_entirely(self, tmp_path):
        dest = tmp_path / "dest"
        zip_content = self._malicious_zip(
            member_name="/tmp/evil.txt", content=b"col1,col2\n1,2\n"
        )

        _save_or_return_parquet_files_from_content(zip_content, save_to_path=dest)

        assert not Path("/tmp/evil.txt").exists()
        assert list(dest.glob("**/*")) == []

    def test_unsafe_member_name_is_skipped_not_written(self, mocker, tmp_path):
        """A member that sanitizes to `None` is skipped, not written."""
        dest = tmp_path / "dest"
        df = pd.DataFrame({"a": [1]})
        buf = io.BytesIO()
        df.to_parquet(buf)
        zip_content = self._malicious_zip(
            member_name="whatever.parquet", content=buf.getvalue()
        )
        mocker.patch(
            "oda_reader.download.download_tools._safe_member_path",
            return_value=None,
        )

        result = _save_or_return_parquet_files_from_content(
            zip_content, save_to_path=dest
        )

        assert result is None
        assert list(dest.glob("*")) == []

    def test_nested_members_preserve_structure_without_collision(self, tmp_path):
        """Two legitimate members with the same filename in different
        subdirectories must both survive -- the exact case flattening to
        a basename would have silently collided on."""
        dest = tmp_path / "dest"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            z.writestr("a/data.txt", "col1,col2\n1,2\n")
            z.writestr("b/data.txt", "col1,col2\n3,4\n")

        _save_or_return_parquet_files_from_content(buffer.getvalue(), save_to_path=dest)

        assert (dest / "a" / "data.parquet").exists()
        assert (dest / "b" / "data.parquet").exists()
        assert pd.read_parquet(dest / "a" / "data.parquet")["col1"].tolist() == [1]
        assert pd.read_parquet(dest / "b" / "data.parquet")["col1"].tolist() == [3]

    def test_genuine_collision_raises_instead_of_overwriting(self, tmp_path):
        """Two members that resolve to the *same* destination path (here,
        via the csv->parquet name-cleaning step colliding, not the zip
        path itself) must error, not silently let the second overwrite
        the first."""
        dest = tmp_path / "dest"
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            z.writestr("Data A.txt", "col1,col2\n1,2\n")
            z.writestr("data_a.txt", "col1,col2\n9,9\n")

        with pytest.raises(ValueError, match="already used by another member"):
            _save_or_return_parquet_files_from_content(
                buffer.getvalue(), save_to_path=dest
            )

    def test_case_insensitive_collision_raises_instead_of_overwriting(self, tmp_path):
        """`Path.__eq__` is case-sensitive regardless of the underlying
        filesystem, but APFS (macOS) and NTFS (Windows) are not --
        "A/data.parquet" and "a/data.parquet" are two different `Path`s
        that are the same file there. The collision guard must catch this
        even though a plain `Path` set wouldn't."""
        dest = tmp_path / "dest"
        df_a = pd.DataFrame({"a": [1]})
        buf_a = io.BytesIO()
        df_a.to_parquet(buf_a)
        df_b = pd.DataFrame({"a": [2]})
        buf_b = io.BytesIO()
        df_b.to_parquet(buf_b)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as z:
            z.writestr("A/data.parquet", buf_a.getvalue())
            z.writestr("a/data.parquet", buf_b.getvalue())

        with pytest.raises(ValueError, match="already used by another member"):
            _save_or_return_parquet_files_from_content(
                buffer.getvalue(), save_to_path=dest
            )


@pytest.mark.unit
class TestDeprecationWarnings:
    """Test deprecation warnings for backward compatibility."""

    @staticmethod
    def _mock_download_pipeline(mocker, tmp_path):
        """Stub the cache manager + content extractor to avoid real downloads.

        The fake path must exist and start with the zip magic bytes, since
        ``bulk_cache_manager().ensure()``'s return value gets its magic bytes
        inspected by ``_consume_bulk_zip`` before any extraction is
        attempted. ``_save_or_return_parquet_files_from_content`` is the
        patch point for the extraction itself.
        """
        fake_path = tmp_path / "fake.zip"
        fake_path.write_bytes(b"PK\x03\x04" + b"\x00" * 16)
        fake_manager = mocker.Mock()
        fake_manager.ensure.return_value = fake_path
        mocker.patch(
            "oda_reader.download.download_tools.bulk_cache_manager",
            return_value=fake_manager,
        )
        mocker.patch(
            "oda_reader.download.download_tools._save_or_return_parquet_files_from_content",
            return_value=[pd.DataFrame({"col": [1, 2]})],
        )

    def test_is_txt_parameter_emits_deprecation_warning(self, mocker, tmp_path):
        """Test that using is_txt parameter emits a deprecation warning."""
        self._mock_download_pipeline(mocker, tmp_path)

        with pytest.warns(DeprecationWarning, match="is_txt.*deprecated"):
            bulk_download_parquet("https://example.oecd.org/fake-id", is_txt=True)

    def test_is_txt_false_also_emits_warning(self, mocker, tmp_path):
        """Test that is_txt=False also emits deprecation warning."""
        self._mock_download_pipeline(mocker, tmp_path)

        with pytest.warns(DeprecationWarning, match="is_txt.*deprecated"):
            bulk_download_parquet("https://example.oecd.org/fake-id", is_txt=False)

    def test_no_warning_when_is_txt_not_provided(self, mocker, tmp_path):
        """Test that no warning is emitted when is_txt is not provided."""
        self._mock_download_pipeline(mocker, tmp_path)

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            bulk_download_parquet("https://example.oecd.org/fake-id")

    def test_file_id_parameter_emits_deprecation_warning_and_is_treated_as_url(
        self, mocker, tmp_path
    ):
        """The deprecated `file_id` kwarg still works, treated as a URL."""
        self._mock_download_pipeline(mocker, tmp_path)

        with pytest.warns(DeprecationWarning, match="file_id.*deprecated"):
            result = bulk_download_parquet(file_id="https://example.oecd.org/fake-id")

        assert isinstance(result, pd.DataFrame)

    def test_url_and_file_id_together_raises(self, mocker, tmp_path):
        """Passing both `url` and the deprecated `file_id` is rejected."""
        self._mock_download_pipeline(mocker, tmp_path)

        with (
            pytest.warns(DeprecationWarning),
            pytest.raises(ValueError, match="not both"),
        ):
            bulk_download_parquet(
                "https://example.oecd.org/a", file_id="https://example.oecd.org/b"
            )

    def test_no_url_or_file_id_raises(self):
        """Calling with neither `url` nor `file_id` is a clear ValueError."""
        with pytest.raises(ValueError, match="requires a 'url'"):
            bulk_download_parquet()


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
LABEL = "CRS-Parquet"
RESOLVED_URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"


def _annotation_xml(label_with_suffix: str, url: str) -> str:
    """A minimal dataflow XML fragment with one EN EXT_RESOURCE annotation."""
    return (
        '<common:AnnotationText xml:lang="en">'
        f"{label_with_suffix}|{url}"
        "</common:AnnotationText>"
    )


@pytest.mark.unit
def test_get_bulk_file_id_raises_runtime_error():
    """The retired get_bulk_file_id always raises, pointing at get_bulk_file_url."""
    with pytest.raises(RuntimeError, match="get_bulk_file_url"):
        get_bulk_file_id(FLOW_URL, LABEL)


@pytest.mark.unit
class TestGetBulkFileUrl:
    """Test get_bulk_file_url with discovery and fallback paths."""

    @pytest.fixture(autouse=True)
    def _no_revalidation_head(self, mocker):
        """Several tests below resolve a bare `LABEL` (no `-vYYYYMMDD`
        suffix), which makes a successful resolution fall back to a live
        `_fetch_revalidation_token` HEAD request. None of these fixtures
        point at a real reachable host, so stub it out for the whole class
        rather than per-test -- keeps this test module offline and fast
        regardless of which label shape an individual test happens to use.
        """
        mocker.patch(
            "oda_reader.download.download_tools._fetch_revalidation_token",
            return_value=None,
        )

    def test_explicit_version_succeeds_immediately(self, mocker):
        """When latest_flow is provided and works, discovery is not called."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(
                200,
                _annotation_xml(f"{LABEL}-v20260803", RESOLVED_URL),
                False,
            ),
        )
        mock_discover = mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
        )

        result = get_bulk_file_url(FLOW_URL, LABEL, latest_flow=1.6)

        assert result == RESOLVED_URL
        mock_discover.assert_not_called()

    def test_discovery_succeeds_when_no_explicit_version(self, mocker):
        """When latest_flow=None, discovery is used and succeeds."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(200, _annotation_xml(LABEL, RESOLVED_URL), False),
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            return_value="1.7",
        )

        result = get_bulk_file_url(FLOW_URL, LABEL)
        assert result == RESOLVED_URL

    def test_explicit_version_fails_then_discovery_rescues(self, mocker):
        """When explicit version fails, discovery finds a working version."""
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            side_effect=[
                (404, "Not found", False),  # explicit version fails
                (200, _annotation_xml(LABEL, RESOLVED_URL), False),  # discovered works
            ],
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            return_value="1.7",
        )

        result = get_bulk_file_url(FLOW_URL, LABEL, latest_flow=1.8)
        assert result == RESOLVED_URL

    def test_discovery_fails_then_scan_rescues(self, mocker):
        """When discovery raises, the decrement scan finds a working version."""
        responses = [(404, "Not found", False)] * 5 + [
            (200, _annotation_xml(LABEL, RESOLVED_URL), False),
        ]
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            side_effect=responses,
        )
        mocker.patch(
            "oda_reader.download.download_tools.discover_latest_version",
            side_effect=ConnectionError("metadata endpoint down"),
        )

        result = get_bulk_file_url(FLOW_URL, LABEL, latest_flow=2.0)
        assert result == RESOLVED_URL

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
            get_bulk_file_url(FLOW_URL, LABEL, latest_flow=1.0)

    def test_label_collision_resolved_exactly_not_by_substring(self, mocker):
        """'CRS-Parquet' must not match inside 'CRS-reduced-parquet' -- the
        substring bug the old SEARCH= matcher had."""
        xml = _annotation_xml(
            "CRS-reduced-parquet-v20260803",
            "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS-reduced.parquet",
        ) + _annotation_xml(f"{LABEL}-v20260803", RESOLVED_URL)
        mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(200, xml, False),
        )

        result = get_bulk_file_url(FLOW_URL, LABEL, latest_flow=1.6)

        assert result == RESOLVED_URL

    def test_bypass_annotation_cache_forces_refresh(self, mocker):
        """bypass_annotation_cache=True is threaded through to force_refresh."""
        mock_get = mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(200, _annotation_xml(LABEL, RESOLVED_URL), False),
        )

        get_bulk_file_url(
            FLOW_URL, LABEL, latest_flow=1.6, bypass_annotation_cache=True
        )

        assert mock_get.call_args.kwargs["force_refresh"] is True

    def test_annotation_cache_not_bypassed_by_default(self, mocker):
        """Without the flag, force_refresh stays False."""
        mock_get = mocker.patch(
            "oda_reader.download.download_tools._get_response_text",
            return_value=(200, _annotation_xml(LABEL, RESOLVED_URL), False),
        )

        get_bulk_file_url(FLOW_URL, LABEL, latest_flow=1.6)

        assert mock_get.call_args.kwargs["force_refresh"] is False


@pytest.mark.unit
class TestParseExtResourcesRealFixtures:
    """`_parse_ext_resources` against real captured dataflow XML.

    These are the actual XML payloads OECD served for each dataflow at
    capture time, not synthetic fragments -- they exercise the label
    suffix-stripping and namespace-prefix handling against the real thing.
    """

    @staticmethod
    def _load(name: str) -> str:
        return (_FIXTURES_DIR / name).read_text(encoding="utf-8")

    def test_crs_version_suffixed_labels_resolve_to_different_urls(self):
        """CRS-Parquet and CRS-reduced-parquet carry a -vYYYYMMDD suffix in
        the live XML and must resolve to two distinct URLs -- the exact case
        the old substring matcher got wrong."""
        resources = _parse_ext_resources(self._load("dataflow_crs.xml"))

        full = resources["CRS-Parquet"]
        reduced = resources["CRS-reduced-parquet"]

        assert (
            full.url == "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"
        )
        assert (
            reduced.url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS-reduced.parquet"
        )
        assert full.url != reduced.url
        # Both carry the live fixture's version token -- this is the value
        # that must end up in CacheEntry.version so a republish (a new
        # token under the same, now-permanently-stable URL) forces a
        # refetch instead of being indistinguishable from a cache hit.
        assert full.version == "v20260803"
        assert reduced.version == "v20260803"

    def test_crs_year_zip_label(self):
        resources = _parse_ext_resources(self._load("dataflow_crs.xml"))
        resource = resources["CRS 2024 (dotStat format)"]
        assert (
            resource.url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS 2024 data.zip"
        )
        assert resource.version == "v20260803"

    def test_dac2a_unsuffixed_label(self):
        """The DAC2A fixture's label carries no -vYYYYMMDD suffix at all --
        the lookup must still match it as-is, and the version token must be
        None (get_bulk_file_url_with_version falls back to an ETag/
        Last-Modified HEAD request for this case, not tested here)."""
        resources = _parse_ext_resources(self._load("dataflow_dac2a.xml"))
        resource = resources["DAC2A full dataset (dotStat format)"]
        assert (
            resource.url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip"
        )
        assert resource.version is None

    def test_dac2b_unsuffixed_label(self):
        """The DAC2B fixture's label carries no -vYYYYMMDD suffix at all --
        the lookup must still match it as-is, and the version token must be
        None (get_bulk_file_url_with_version falls back to an ETag/
        Last-Modified HEAD request for this case, not tested here)."""
        resources = _parse_ext_resources(self._load("dataflow_dac2b.xml"))
        resource = resources["DAC2B full dataset (dotStat format)"]
        assert (
            resource.url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2b_Data.zip"
        )
        assert resource.version is None

    def test_multisystem_suffixed_label(self):
        resources = _parse_ext_resources(self._load("dataflow_multi.xml"))
        resource = resources["Entire dataset (dotStat format)"]
        assert (
            resource.url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_MULTI/MultiSystem entire dataset.zip"
        )
        assert resource.version == "v20260710"

    def test_cpa_suffixed_label(self):
        resources = _parse_ext_resources(self._load("dataflow_cpa.xml"))
        resource = resources["CRS CPA 2024 (dotStat format)"]
        assert (
            resource.url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRSCPA/CrsCPA 2024 data.zip"
        )
        assert resource.version == "v20260710"

    def test_dac1_legacy_fileview_url_unsuffixed(self):
        """DAC1's fixture predates the URL migration (still a fileview2.aspx
        GUID link, no -vYYYYMMDD suffix) -- parsing must not choke on it."""
        resources = _parse_ext_resources(self._load("dataflow_dac1.xml"))
        resource = resources["DAC1 full dataset (dotStat format)"]
        assert resource.url == (
            "https://stats.oecd.org/wbos/fileview2.aspx?"
            "IDFile=570b240e-df9c-4586-b53d-99946fac437d"
        )
        assert resource.version is None

    def test_same_day_republish_suffix_variant(self):
        """No captured fixture has been republished same-day yet, so the
        -vYYYYMMDD-N tail is exercised directly here."""
        xml = _annotation_xml(f"{LABEL}-v20260803-1", RESOLVED_URL)
        resources = _parse_ext_resources(xml)
        assert resources[LABEL].url == RESOLVED_URL
        assert resources[LABEL].version == "v20260803-1"


@pytest.mark.unit
class TestFinalizeResolvedUrl:
    """`_finalize_resolved_url`: encoding and the host allowlist."""

    def test_encodes_spaces_in_path(self):
        url = _finalize_resolved_url(
            "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS 2024 data.zip"
        )
        assert (
            url
            == "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS%202024%20data.zip"
        )

    def test_does_not_double_encode_an_already_encoded_url(self):
        already_encoded = (
            "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS%202024%20data.zip"
        )
        url = _finalize_resolved_url(already_encoded)
        assert url == already_encoded
        assert "%2520" not in url

    def test_rejects_non_oecd_host(self):
        with pytest.raises(ValueError, match="untrusted host"):
            _finalize_resolved_url("https://evil.example.com/CRS.parquet")

    def test_rejects_non_https_scheme(self):
        with pytest.raises(ValueError, match="untrusted host"):
            _finalize_resolved_url("http://webfs-dcd.oecd.org/files/x.zip")

    def test_allows_any_oecd_org_subdomain(self):
        url = _finalize_resolved_url("https://sdmx.oecd.org/files/x.zip")
        assert url == "https://sdmx.oecd.org/files/x.zip"


@pytest.mark.unit
class TestFetchRevalidationToken:
    """`_fetch_revalidation_token`: the ETag/Last-Modified HEAD fallback for
    a label with no `-vYYYYMMDD` suffix (DAC2A today)."""

    _URL = "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip"

    def test_success_returns_etag(self, mocker):
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.head.return_value = mocker.Mock(
            status_code=200,
            headers={"ETag": '"abc123"', "Last-Modified": "Tue, 07 Jul 2026"},
        )
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        assert _fetch_revalidation_token(self._URL) == '"abc123"'

    def test_falls_back_to_last_modified_when_no_etag(self, mocker):
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.head.return_value = mocker.Mock(
            status_code=200,
            headers={"Last-Modified": "Tue, 07 Jul 2026 13:05:26 GMT"},
        )
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        result = _fetch_revalidation_token(self._URL)

        assert result == "Tue, 07 Jul 2026 13:05:26 GMT"

    def test_both_headers_absent_returns_none(self, mocker):
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.head.return_value = mocker.Mock(status_code=200, headers={})
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        assert _fetch_revalidation_token(self._URL) is None

    def test_non_2xx_returns_none(self, mocker):
        """A 403 (e.g. the Cloudflare challenge on a HEAD without the full
        browser-like header set) degrades to None, not an exception."""
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.head.return_value = mocker.Mock(
            status_code=403, headers={"ETag": '"should-be-ignored"'}
        )
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        assert _fetch_revalidation_token(self._URL) is None

    def test_transport_error_returns_none(self, mocker):
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.head.side_effect = requests.exceptions.ConnectionError("boom")
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        assert _fetch_revalidation_token(self._URL) is None

    def test_untrusted_host_returns_none_without_a_request(self, mocker):
        """The host allowlist applies here too -- rejected before any
        request goes out, same as the main fetch path."""
        session = mocker.Mock()
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        assert _fetch_revalidation_token("https://evil.example.com/f.zip") is None
        session.head.assert_not_called()

    def test_rate_limiter_is_invoked(self, mocker):
        """Every other network call this package makes goes through
        API_RATE_LIMITER.wait() first; this HEAD must too."""
        mock_wait = mocker.patch(
            "oda_reader.download.download_tools.API_RATE_LIMITER.wait"
        )
        session = mocker.Mock()
        session.head.return_value = mocker.Mock(status_code=200, headers={})
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        _fetch_revalidation_token(self._URL)

        mock_wait.assert_called_once()


class _FakeStreamResponse:
    """A minimal stand-in for `requests.Response` used as a context manager."""

    def __init__(
        self,
        status_code: int,
        chunks: list[bytes] | None = None,
        text: str = "",
        headers: dict | None = None,
    ):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self._chunks = chunks or []

    def iter_content(self, chunk_size: int = 8192):
        return iter(self._chunks)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


@pytest.mark.unit
class TestStreamToFileRetry:
    """`_stream_to_file`'s 403 retry-with-backoff and 404 fail-fast."""

    @staticmethod
    def _fake_response(
        status_code: int,
        chunks: list[bytes] | None = None,
        text: str = "",
        headers: dict | None = None,
    ):
        return _FakeStreamResponse(
            status_code, chunks=chunks, text=text, headers=headers
        )

    def test_403_then_200_retries_and_succeeds(self, mocker, tmp_path):
        mocker.patch("oda_reader.download.download_tools.time.sleep")
        # The global rate limiter accumulates call timestamps across the
        # whole test session; stub it out so its own internal sleep() calls
        # (unrelated to this test's retry backoff) can't pollute the
        # assertions below.
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.side_effect = [
            self._fake_response(403),
            self._fake_response(200, chunks=[b"chunk1", b"chunk2"]),
        ]
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )
        target = tmp_path / "out.bin"

        _stream_to_file("https://sdmx.oecd.org/f.zip", {}, target)

        assert target.read_bytes() == b"chunk1chunk2"
        assert session.get.call_count == 2

    def test_404_fails_fast_with_no_backoff(self, mocker, tmp_path):
        mock_sleep = mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.return_value = self._fake_response(404, text="not found")
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        with pytest.raises(BulkDownloadHTTPError) as exc_info:
            _stream_to_file("https://sdmx.oecd.org/f.zip", {}, tmp_path / "out.bin")

        assert exc_info.value.status_code == 404
        assert session.get.call_count == 1
        mock_sleep.assert_not_called()

    def test_malformed_url_fails_fast_with_no_backoff(self, mocker, tmp_path):
        """A permanent, request-is-malformed error is not retried.

        `MissingSchema`/`InvalidURL`/`InvalidSchema` can never succeed on a
        retry -- unlike `ConnectionError`/`Timeout`/etc., which are
        genuinely transient. In practice `_validate_allowed_host` already
        rejects a URL malformed enough to trip these before `requests` is
        ever called; this exercises the narrowed exception catch directly
        (`session.get` raising) as the defense-in-depth backstop for
        whatever that earlier check doesn't anticipate.
        """
        mock_sleep = mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.side_effect = requests.exceptions.MissingSchema(
            "Invalid URL '570b240e-df9c-4586-b53d-99946fac437d': No scheme supplied."
        )
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        with pytest.raises(requests.exceptions.MissingSchema):
            _stream_to_file("https://sdmx.oecd.org/f.zip", {}, tmp_path / "out.bin")

        assert session.get.call_count == 1
        mock_sleep.assert_not_called()

    def test_403_exhausts_all_retries_then_raises(self, mocker, tmp_path):
        mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.return_value = self._fake_response(403, text="challenge")
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        with pytest.raises(BulkDownloadHTTPError) as exc_info:
            _stream_to_file("https://sdmx.oecd.org/f.zip", {}, tmp_path / "out.bin")

        assert exc_info.value.status_code == 403
        # 1 initial attempt + 3 backoff retries, matching
        # _STREAM_RETRY_BACKOFF_SECONDS = (1, 2, 4).
        assert session.get.call_count == 4

    def test_truncated_download_retries_and_succeeds(self, mocker, tmp_path):
        """Bytes written short of Content-Length is retried, not a hard failure."""
        mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.side_effect = [
            self._fake_response(
                200, chunks=[b"chunk1"], headers={"Content-Length": "12"}
            ),
            self._fake_response(
                200,
                chunks=[b"chunk1", b"chunk2"],
                headers={"Content-Length": "12"},
            ),
        ]
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )
        target = tmp_path / "out.bin"

        _stream_to_file("https://sdmx.oecd.org/f.zip", {}, target)

        assert target.read_bytes() == b"chunk1chunk2"
        assert session.get.call_count == 2

    def test_truncated_download_exhausts_retries_then_raises(self, mocker, tmp_path):
        mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.return_value = self._fake_response(
            200, chunks=[b"chunk1"], headers={"Content-Length": "12"}
        )
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )

        with pytest.raises(BulkDownloadHTTPError) as exc_info:
            _stream_to_file("https://sdmx.oecd.org/f.zip", {}, tmp_path / "out.bin")

        assert "Truncated" in exc_info.value.body
        assert session.get.call_count == 4

    def test_gzip_encoded_response_skips_truncation_check(self, mocker, tmp_path):
        """Content-Length is the *compressed* size for a gzip response, but
        iter_content hands back decompressed bytes -- comparing the two
        would always false-positive, so the check must be skipped."""
        mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.return_value = self._fake_response(
            200,
            chunks=[b"decompressed content longer than Content-Length"],
            headers={"Content-Length": "10", "Content-Encoding": "gzip"},
        )
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )
        target = tmp_path / "out.bin"

        _stream_to_file("https://sdmx.oecd.org/f.zip", {}, target)

        assert target.read_bytes() == b"decompressed content longer than Content-Length"
        assert session.get.call_count == 1

    def test_missing_content_length_skips_truncation_check(self, mocker, tmp_path):
        mocker.patch("oda_reader.download.download_tools.time.sleep")
        mocker.patch("oda_reader.download.download_tools.API_RATE_LIMITER.wait")
        session = mocker.Mock()
        session.get.return_value = self._fake_response(200, chunks=[b"whatever"])
        mocker.patch(
            "oda_reader.download.download_tools._get_bulk_stream_session",
            return_value=session,
        )
        target = tmp_path / "out.bin"

        _stream_to_file("https://sdmx.oecd.org/f.zip", {}, target)

        assert target.read_bytes() == b"whatever"
        assert session.get.call_count == 1


@pytest.mark.unit
class TestCheckTruncated:
    """`_check_truncated`: the O(1) Content-Length comparison directly."""

    def test_matching_length_is_not_truncated(self):
        headers = {"Content-Length": "5"}
        assert _check_truncated(headers, 5) is None

    def test_short_write_is_truncated(self):
        headers = {"Content-Length": "10"}
        result = _check_truncated(headers, 3)
        assert result is not None
        assert "3" in result and "10" in result

    def test_missing_content_length_skips_check(self):
        assert _check_truncated({}, 3) is None

    def test_content_encoding_present_skips_check(self):
        headers = {"Content-Length": "10", "Content-Encoding": "gzip"}
        assert _check_truncated(headers, 3) is None

    def test_unparseable_content_length_skips_check(self):
        headers = {"Content-Length": "not-a-number"}
        assert _check_truncated(headers, 3) is None


@pytest.mark.unit
class TestValidatedRedirects:
    """`_get_with_validated_redirects`: redirects re-checked against the allowlist."""

    @staticmethod
    def _fake_response(status_code: int, location: str | None = None, **kwargs):
        headers = {"Location": location} if location else {}
        return _FakeStreamResponse(status_code, headers=headers, **kwargs)

    def test_terminal_2xx_returned_without_following(self, mocker):
        session = mocker.Mock()
        session.get.return_value = self._fake_response(200, chunks=[b"data"])

        response = _get_with_validated_redirects(
            session, "https://webfs-dcd.oecd.org/f.zip", {}, timeout=(10, 60)
        )

        assert response.status_code == 200
        assert session.get.call_count == 1

    def test_same_allowlist_redirect_is_followed(self, mocker):
        session = mocker.Mock()
        session.get.side_effect = [
            self._fake_response(302, location="https://webfs-dcd.oecd.org/moved.zip"),
            self._fake_response(200, chunks=[b"data"]),
        ]

        response = _get_with_validated_redirects(
            session, "https://webfs-dcd.oecd.org/f.zip", {}, timeout=(10, 60)
        )

        assert response.status_code == 200
        assert session.get.call_count == 2

    def test_cross_host_redirect_raises_before_following(self, mocker):
        session = mocker.Mock()
        session.get.return_value = self._fake_response(
            302, location="https://evil.example.com/payload.zip"
        )

        with pytest.raises(ValueError, match="untrusted host"):
            _get_with_validated_redirects(
                session, "https://webfs-dcd.oecd.org/f.zip", {}, timeout=(10, 60)
            )

        # The malicious target is never requested -- rejected on inspection
        # of the Location header, before a second call is made.
        assert session.get.call_count == 1

    def test_redirect_with_no_location_raises(self, mocker):
        session = mocker.Mock()
        session.get.return_value = self._fake_response(302, location=None)

        with pytest.raises(BulkDownloadHTTPError, match="no Location"):
            _get_with_validated_redirects(
                session, "https://webfs-dcd.oecd.org/f.zip", {}, timeout=(10, 60)
            )

    def test_redirect_chain_exceeding_hop_cap_raises(self, mocker):
        session = mocker.Mock()
        session.get.side_effect = [
            self._fake_response(302, location=f"https://webfs-dcd.oecd.org/hop{i}.zip")
            for i in range(10)
        ]

        with pytest.raises(BulkDownloadHTTPError, match="Exceeded"):
            _get_with_validated_redirects(
                session, "https://webfs-dcd.oecd.org/f.zip", {}, timeout=(10, 60)
            )

    def test_initial_url_off_allowlist_raises_without_any_request(self, mocker):
        session = mocker.Mock()

        with pytest.raises(ValueError, match="untrusted host"):
            _get_with_validated_redirects(
                session, "https://evil.example.com/f.zip", {}, timeout=(10, 60)
            )

        session.get.assert_not_called()


@pytest.mark.unit
class TestStaleAnnotationRecovery:
    """bulk_download_parquet's re-resolve-and-retry-once on a stale annotation."""

    def test_404_triggers_reresolve_with_bypass_and_single_retry(self, mocker):
        stale_url = "https://sdmx.oecd.org/stale.zip"
        fresh_url = "https://sdmx.oecd.org/fresh.zip"
        calls: list[str] = []

        def fake_stream(url: str, headers: dict, path) -> None:
            calls.append(url)
            if url == stale_url:
                raise BulkDownloadHTTPError(status_code=404, url=url, body="gone")
            with zipfile.ZipFile(path, "w") as zf:
                zf.writestr("data.parquet", b"\x00")

        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=fake_stream,
        )
        mock_resolve = mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            return_value=(fresh_url, "v20260804"),
        )
        mocker.patch(
            "oda_reader.download.download_tools._consume_bulk_zip",
            return_value=pd.DataFrame({"a": [1]}),
        )

        result = bulk_download_parquet(
            url=stale_url,
            flow_url=FLOW_URL,
            label=LABEL,
            use_raw_cache=False,
        )

        assert calls == [stale_url, fresh_url]
        mock_resolve.assert_called_once_with(
            FLOW_URL, LABEL, None, bypass_annotation_cache=True
        )
        assert isinstance(result, pd.DataFrame)

    def test_second_failure_after_reresolve_propagates(self, mocker):
        """Only a single retry is attempted -- a repeat failure must not loop."""
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=BulkDownloadHTTPError(
                status_code=404, url="https://sdmx.oecd.org/x.zip", body="gone"
            ),
        )
        mock_resolve = mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            return_value=("https://sdmx.oecd.org/still-dead.zip", None),
        )

        with pytest.raises(BulkDownloadHTTPError):
            bulk_download_parquet(
                url="https://sdmx.oecd.org/x.zip",
                flow_url=FLOW_URL,
                label=LABEL,
                use_raw_cache=False,
            )

        mock_resolve.assert_called_once()

    def test_no_reresolve_without_flow_url_and_label(self, mocker):
        """Without flow_url/label, a 404 just propagates -- nothing to re-resolve with."""
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=BulkDownloadHTTPError(
                status_code=404, url="https://sdmx.oecd.org/x.zip", body="gone"
            ),
        )
        mock_resolve = mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
        )

        with pytest.raises(BulkDownloadHTTPError):
            bulk_download_parquet(
                url="https://sdmx.oecd.org/x.zip", use_raw_cache=False
            )

        mock_resolve.assert_not_called()

    def test_non_404_403_status_is_not_retried(self, mocker):
        """A 500 (or anything outside 404/403) is not treated as stale-annotation."""
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=BulkDownloadHTTPError(
                status_code=500, url="https://sdmx.oecd.org/x.zip", body="oops"
            ),
        )
        mock_resolve = mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
        )

        with pytest.raises(BulkDownloadHTTPError) as exc_info:
            bulk_download_parquet(
                url="https://sdmx.oecd.org/x.zip",
                flow_url=FLOW_URL,
                label=LABEL,
                use_raw_cache=False,
            )

        assert exc_info.value.status_code == 500
        mock_resolve.assert_not_called()


def _write_tiny_parquet(path) -> None:
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(path)


@pytest.mark.unit
class TestBareParquetExtractionViaBulkDownload:
    """Bare (non-zipped) parquet payloads through all three bulk_download_parquet
    modes. Only `_stream_to_file` is mocked (no real network); the real pyarrow
    read/write path runs on a tiny in-test file, so this exercises the actual
    `_consume_bare_parquet` / `_save_or_return_bare_parquet` code, not a stub.
    """

    def test_whole_frame_mode(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=lambda url, headers, path: _write_tiny_parquet(path),
        )

        result = bulk_download_parquet(
            url="https://sdmx.oecd.org/CRS.parquet", use_raw_cache=False
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["a", "b"]

    def test_save_to_path_mode(self, mocker, tmp_path):
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=lambda url, headers, path: _write_tiny_parquet(path),
        )
        dest = tmp_path / "out_dir"

        result = bulk_download_parquet(
            url="https://sdmx.oecd.org/CRS.parquet",
            save_to_path=dest,
            use_raw_cache=False,
        )

        assert result is None
        saved = list(dest.glob("*.parquet"))
        assert len(saved) == 1
        assert len(pd.read_parquet(saved[0])) == 2

    def test_as_iterator_mode(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=lambda url, headers, path: _write_tiny_parquet(path),
        )

        result = bulk_download_parquet(
            url="https://sdmx.oecd.org/CRS.parquet",
            as_iterator=True,
            use_raw_cache=False,
        )

        frames = list(result)
        assert sum(len(f) for f in frames) == 2


@pytest.mark.unit
class TestVersionThreadedInvalidation:
    """`version` (from `get_bulk_file_url_with_version`) actually forces a refetch.

    The URLs OECD now serves are permanently stable (`CRS.parquet` never
    changes, unlike the old GUID URLs that rotated on every republish), so
    a cache keyed on `sha1(url)` alone can no longer tell a republish apart
    from a genuine cache hit -- same key, no version, stale data returned
    silently for up to the TTL. The assertion that matters is not that
    `CacheEntry.version` is populated; it's that changing it actually
    triggers a new fetch.
    """

    def test_same_url_different_version_triggers_refetch(self, tmp_path, mocker):
        manager = CacheManager(base_dir=tmp_path)
        mocker.patch(
            "oda_reader.download.download_tools.bulk_cache_manager",
            return_value=manager,
        )

        fetch_count = {"n": 0}

        def fake_stream(url_, headers, path):
            fetch_count["n"] += 1
            _write_tiny_parquet(path)

        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=fake_stream,
        )

        url = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"

        bulk_download_parquet(url=url, version="v20260803", use_raw_cache=True)
        assert fetch_count["n"] == 1

        # Same URL, called again immediately with the SAME version: a
        # genuine cache hit, no new fetch -- this is the control case that
        # proves the second call below isn't just "always refetches".
        bulk_download_parquet(url=url, version="v20260803", use_raw_cache=True)
        assert fetch_count["n"] == 1

        # Same URL, but the label was republished under a new version token
        # (exactly what a real OECD republish produces via
        # get_bulk_file_url_with_version). This must force a real refetch,
        # not silently reuse the stale cached payload.
        bulk_download_parquet(url=url, version="v20260804", use_raw_cache=True)
        assert fetch_count["n"] == 2

    def test_no_version_falls_back_to_ttl_only(self, tmp_path, mocker):
        """version=None (e.g. DAC2A when its HEAD revalidation also fails)
        must not error -- it degrades to the pre-existing TTL-only behavior."""
        manager = CacheManager(base_dir=tmp_path)
        mocker.patch(
            "oda_reader.download.download_tools.bulk_cache_manager",
            return_value=manager,
        )

        fetch_count = {"n": 0}

        def fake_stream(url_, headers, path):
            fetch_count["n"] += 1
            _write_tiny_parquet(path)

        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=fake_stream,
        )

        url = "https://webfs-dcd.oecd.org/files/dotStat/DSD_DAC2/Table2a_Data.zip"

        bulk_download_parquet(url=url, version=None, use_raw_cache=True)
        bulk_download_parquet(url=url, version=None, use_raw_cache=True)

        assert fetch_count["n"] == 1
        assert manager.list_records()[0]["version"] is None


@pytest.mark.unit
class TestLazyIteratorCorruptionEviction:
    """Corruption discovered mid-iteration (not at eager validation time)
    must still evict the cache entry, matching BULK_PAYLOAD_CORRUPT_HINT's
    promise that "the corrupt entry has been removed". `as_iterator=True`
    hands back a generator; constructing it runs none of its body, so a
    corrupt payload that only fails once actually read used to escape the
    try/except around construction entirely -- surfacing as a raw
    pyarrow/pandas exception, with the cache never cleared, so every future
    call failed identically.

    The end state that matters, per format, is asserted explicitly: not
    just "the right exception type comes out", but that the cache entry is
    actually gone afterward and a subsequent call re-fetches cleanly.
    """

    def test_bare_parquet_iterator_corruption_evicts_and_refetches(
        self, tmp_path, mocker
    ):
        """CRS.parquet-style bare payload: footer validates, a row group doesn't."""
        manager = CacheManager(base_dir=tmp_path)
        mocker.patch(
            "oda_reader.download.download_tools.bulk_cache_manager",
            return_value=manager,
        )

        fetch_count = {"n": 0}

        def fake_stream(url_, headers, path):
            fetch_count["n"] += 1
            _write_tiny_parquet(path)

        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=fake_stream,
        )

        # Footer-only validation (CacheManager.ensure -> validate_payload_or_raise)
        # can't catch this -- it deliberately doesn't walk row-group data --
        # so simulate the corruption at the one place it actually surfaces:
        # a row-group read, which only runs once the iterator is consumed.
        should_corrupt = {"value": True}
        real_read_row_group = pq.ParquetFile.read_row_group

        def maybe_corrupt_read(self, i, *args, **kwargs):
            if should_corrupt["value"]:
                raise pyarrow.ArrowInvalid("simulated mid-file corruption")
            return real_read_row_group(self, i, *args, **kwargs)

        mocker.patch.object(pq.ParquetFile, "read_row_group", maybe_corrupt_read)

        url = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS.parquet"

        it = bulk_download_parquet(url=url, as_iterator=True, use_raw_cache=True)
        with pytest.raises(BulkPayloadCorruptError):
            list(it)

        assert fetch_count["n"] == 1
        assert manager.list_records() == []
        assert list(tmp_path.glob("*.parquet")) == []

        # Corruption is gone; a subsequent call must re-fetch cleanly rather
        # than reuse a stale (and by now nonexistent) cache entry.
        should_corrupt["value"] = False
        it2 = bulk_download_parquet(url=url, as_iterator=True, use_raw_cache=True)
        chunks = list(it2)

        assert fetch_count["n"] == 2
        assert sum(len(c) for c in chunks) == 2

    def test_delimited_iterator_corruption_evicts_and_refetches(self, tmp_path, mocker):
        """CRS-per-year-style zip payload: zip CRC validates, a CSV row doesn't."""
        manager = CacheManager(base_dir=tmp_path)
        mocker.patch(
            "oda_reader.download.download_tools.bulk_cache_manager",
            return_value=manager,
        )

        fetch_count = {"n": 0}

        def fake_stream(url_, headers, path):
            fetch_count["n"] += 1
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w") as z:
                # A well-formed zip (valid CRC) -- validate_payload_or_raise
                # passes -- containing a CSV row with the wrong field count.
                # The C parser only discovers that once it actually reads
                # the row, i.e. mid-iteration, not at construction time.
                content = (
                    "col1,col2,col3\n1,2,3\n1,2,3,4,5\n"
                    if fetch_count["n"] == 1
                    else "col1,col2,col3\n1,2,3\n4,5,6\n"
                )
                z.writestr("CRS 2024 data.txt", content.encode("utf-8"))
            path.write_bytes(buf.getvalue())

        mocker.patch(
            "oda_reader.download.download_tools._stream_to_file",
            side_effect=fake_stream,
        )

        url = "https://webfs-dcd.oecd.org/files/dotStat/DSD_CRS/CRS 2024 data.zip"

        it = bulk_download_parquet(url=url, as_iterator=True, use_raw_cache=True)
        with pytest.raises(BulkPayloadCorruptError):
            list(it)

        assert fetch_count["n"] == 1
        assert manager.list_records() == []
        assert list(tmp_path.glob("*.zip")) == []

        # Corruption is gone from the second write; a subsequent call must
        # re-fetch cleanly rather than reuse a stale (and by now
        # nonexistent) cache entry.
        it2 = bulk_download_parquet(url=url, as_iterator=True, use_raw_cache=True)
        chunks = list(it2)

        assert fetch_count["n"] == 2
        combined = pd.concat(chunks, ignore_index=True)
        assert len(combined) == 2
        assert combined["col1"].tolist() == [1, 4]
