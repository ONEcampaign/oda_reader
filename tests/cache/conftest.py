"""Shared fixtures for oda_reader cache tests."""

import io
import os
import zipfile
from pathlib import Path

import pytest


@pytest.fixture()
def tmp_cache_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Temporary cache root isolated from real cache.

    Sets ODA_READER_CACHE_DIR to a unique tmpdir so tests never touch
    the user's real cache. Yields the root path; env var is restored on
    teardown by monkeypatch automatically.

    Args:
        monkeypatch: pytest monkeypatch fixture.
        tmp_path: pytest temporary directory.

    Yields:
        Path: Temporary cache root directory.
    """
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ODA_READER_CACHE_DIR", str(cache_dir))
    yield cache_dir


def _make_valid_zip(target: Path) -> None:
    """Write a ~1 KB valid zip containing one parquet-like entry."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        # Minimal PAR1 magic + padding to reach ~1 KB
        zf.writestr("data.parquet", b"PAR1" + b"\x00" * 1000)
    target.write_bytes(buf.getvalue())


@pytest.fixture()
def valid_tiny_zip(tmp_path: Path) -> Path:
    """Path to a ~1 KB valid zip file with one parquet-like entry.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path: Path to the valid zip file.
    """
    path = tmp_path / "valid.zip"
    _make_valid_zip(path)
    return path


@pytest.fixture()
def corrupt_zip_file(tmp_path: Path) -> Path:
    """Path to a non-zip 1 KB file for corruption tests.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path: Path to the corrupt (non-zip) file.
    """
    path = tmp_path / "corrupt.zip"
    path.write_bytes(b"NOT A ZIP" + b"\xff" * 1000)
    return path


@pytest.fixture()
def monkeypatched_fetcher():
    """Return a callable writing a 1 KB valid zip to any target path.

    The returned callable matches the ``fetcher(target_path: Path)``
    signature expected by ``CacheManager.ensure``.

    Returns:
        Callable[[Path], None]: Fake fetcher writing a valid zip.
    """

    def _fetcher(target_path: Path) -> None:
        _make_valid_zip(target_path)

    return _fetcher


@pytest.fixture()
def skip_if_no_network() -> None:
    """Skip the current test when RUN_NETWORK_TESTS is not set to '1'."""
    if os.environ.get("RUN_NETWORK_TESTS") != "1":
        pytest.skip("set RUN_NETWORK_TESTS=1 to run network tests")
