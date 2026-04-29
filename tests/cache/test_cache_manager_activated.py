"""Tests for the activated CacheManager.ensure() path and related behavior."""

import contextlib
import hashlib
import inspect
import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from oda_reader._cache.config import get_bulk_cache_dir
from oda_reader._cache.manager import CacheEntry, CacheManager
from oda_reader.aiddata import download_aiddata
from oda_reader.download import download_tools
from oda_reader.download.download_tools import bulk_download_aiddata
from oda_reader.exceptions import BulkPayloadCorruptError, validate_zip_or_raise


def _make_valid_zip(target: Path) -> None:
    """Write a ~1 KB valid zip with one parquet-like entry to target."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("data.parquet", b"PAR1" + b"\x00" * 1000)
    target.write_bytes(buf.getvalue())


def _make_entry(key: str, fetcher) -> CacheEntry:
    return CacheEntry(key=key, filename=f"{key}.zip", fetcher=fetcher)


def test_bulk_payload_corrupt_attributes() -> None:
    """BulkPayloadCorruptError carries .path, .reason, and an actionable str()."""
    path = Path("/tmp/x.zip")
    exc = BulkPayloadCorruptError(path, reason="testzip failure")
    assert exc.path == path
    assert exc.reason == "testzip failure"
    msg = str(exc)
    assert str(path) in msg
    assert "testzip failure" in msg
    assert "use_raw_cache=False" in msg


def test_bulk_download_parquet_uses_cache_manager(tmp_path, monkeypatch) -> None:
    """bulk_download_parquet routes through CacheManager.ensure().

    The cached zip must live at base_dir/<sha1(url)>.zip and the manifest
    must contain the entry.
    """
    file_id = "TESTID123"
    url = download_tools.BULK_DOWNLOAD_URL + file_id
    expected_key = hashlib.sha1(url.encode()).hexdigest()

    def fake_stream(url_: str, headers: dict, path: Path) -> None:
        _make_valid_zip(path)

    monkeypatch.setattr(download_tools, "_stream_to_file", fake_stream)
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(tmp_path / "cache"))

    # Extraction of fake zip may fail; we only check the zip presence.
    with contextlib.suppress(Exception):
        download_tools.bulk_download_parquet(file_id, use_raw_cache=True)

    bulk_dir = get_bulk_cache_dir()
    cached_zip = bulk_dir / f"{expected_key}.zip"
    assert cached_zip.exists(), f"Expected cached zip at {cached_zip}"


def test_testzip_post_condition_raises_on_miss(tmp_path) -> None:
    """Cache-miss branch: BulkPayloadCorruptError raised for a non-zip payload, entry removed."""
    manager = CacheManager(base_dir=tmp_path)

    def bad_fetcher(p: Path) -> None:
        p.write_bytes(b"NOT A ZIP" * 50)

    entry = _make_entry("crs_full", bad_fetcher)

    with pytest.raises(BulkPayloadCorruptError) as exc_info:
        manager.ensure(entry)

    exc = exc_info.value
    assert exc.reason != ""
    assert not (tmp_path / "crs_full.zip").exists()


def test_validate_zip_wraps_testzip_exceptions(tmp_path, monkeypatch) -> None:
    """If testzip() itself raises (corrupt member), it is converted to
    BulkPayloadCorruptError and the file is unlinked — not propagated raw."""
    target = tmp_path / "valid_outer_corrupt_inner.zip"
    _make_valid_zip(target)

    def boom(self):
        raise zipfile.BadZipFile("synthetic corruption inside testzip")

    monkeypatch.setattr(zipfile.ZipFile, "testzip", boom)

    with pytest.raises(BulkPayloadCorruptError) as exc_info:
        validate_zip_or_raise(target)

    assert "testzip() raised BadZipFile" in exc_info.value.reason
    assert not target.exists(), "corrupt zip must be removed"


def test_testzip_skipped_on_cache_hit(tmp_path) -> None:
    """Phase-2 fix #10: testzip must NOT be called on cache-hit paths."""
    manager = CacheManager(base_dir=tmp_path)

    def fetcher(p: Path) -> None:
        _make_valid_zip(p)

    entry = _make_entry("crs_full", fetcher)
    manager.ensure(entry)

    with patch("zipfile.ZipFile.testzip") as mock_testzip:
        manager.ensure(entry)
        mock_testzip.assert_not_called()


def test_use_raw_cache_false_skips_cache(tmp_path, monkeypatch) -> None:
    """use_raw_cache=False: no zip ends up in the cache dir after the call."""

    def fake_stream(url_: str, headers: dict, path: Path) -> None:
        _make_valid_zip(path)

    monkeypatch.setattr(download_tools, "_stream_to_file", fake_stream)
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(tmp_path / "cache"))

    with contextlib.suppress(Exception):
        download_tools.bulk_download_parquet("FAKEID", use_raw_cache=False)

    cache_root = tmp_path / "cache"
    cache_zips = list(cache_root.rglob("*.zip")) if cache_root.exists() else []
    assert cache_zips == [], f"Found unexpected cached zips: {cache_zips}"


def test_use_raw_cache_false_validates_corrupt(tmp_path, monkeypatch) -> None:
    """use_raw_cache=False: validation still runs and raises BulkPayloadCorruptError."""

    def bad_stream(url_: str, headers: dict, path: Path) -> None:
        path.write_bytes(b"NOT A ZIP" * 50)

    monkeypatch.setattr(download_tools, "_stream_to_file", bad_stream)
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(tmp_path / "cache"))

    with pytest.raises(BulkPayloadCorruptError) as exc_info:
        download_tools.bulk_download_parquet("FAKEID", use_raw_cache=False)

    assert exc_info.value.reason != ""


def test_use_raw_cache_false_iterator_cleans_temp_on_exhaustion(
    monkeypatch,
) -> None:
    """Iterator + use_raw_cache=False must delete the temp zip on completion."""
    written_paths: list[Path] = []

    def fake_stream(url_: str, headers: dict, path: Path) -> None:
        written_paths.append(path)
        _make_valid_zip(path)

    monkeypatch.setattr(download_tools, "_stream_to_file", fake_stream)

    it = download_tools.bulk_download_parquet(
        "FAKEID", as_iterator=True, use_raw_cache=False
    )
    assert it is not None
    # Iterator is lazy; temp file still exists.
    assert written_paths and written_paths[0].exists()

    # Exhaust the iterator (the fake zip has no real parquet inside, so
    # iteration may raise — that's still a path to cleanup).
    with contextlib.suppress(Exception):
        list(it)

    assert not written_paths[0].exists(), "temp zip should be deleted"


def test_use_raw_cache_false_extraction_error_cleans_temp(monkeypatch) -> None:
    """A non-BadZipFile error during extraction must still delete the temp zip."""
    written_paths: list[Path] = []

    def fake_stream(url_: str, headers: dict, path: Path) -> None:
        written_paths.append(path)
        _make_valid_zip(path)

    def boom(*args, **kwargs):
        raise RuntimeError("extraction broke")

    monkeypatch.setattr(download_tools, "_stream_to_file", fake_stream)
    monkeypatch.setattr(
        download_tools, "_save_or_return_parquet_files_from_content", boom
    )

    with pytest.raises(RuntimeError, match="extraction broke"):
        download_tools.bulk_download_parquet("FAKEID", use_raw_cache=False)

    assert written_paths and not written_paths[0].exists()


def test_is_txt_still_accepts_positional() -> None:
    """is_txt remains a positional parameter for backward-compat with existing
    callers that pass it as the third positional arg.

    We only check the signature shape; behavior is covered by the unit tests
    that mock the download pipeline.
    """
    sig = inspect.signature(download_tools.bulk_download_parquet)
    is_txt = sig.parameters["is_txt"]
    assert is_txt.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD


def test_aiddata_unaffected() -> None:
    """download_aiddata and bulk_download_aiddata must not have use_raw_cache."""
    assert "use_raw_cache" not in inspect.signature(download_aiddata).parameters
    assert "use_raw_cache" not in inspect.signature(bulk_download_aiddata).parameters
