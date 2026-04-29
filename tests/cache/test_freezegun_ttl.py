"""Tests for CacheManager TTL behavior using frozen clocks."""

import io
import zipfile
from pathlib import Path

import pytest

freeze_time = pytest.importorskip("freezegun").freeze_time

from oda_reader._cache.manager import CacheEntry, CacheManager  # noqa: E402


def _make_valid_zip(target: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("data.parquet", b"PAR1" + b"\x00" * 100)
    target.write_bytes(buf.getvalue())


def _make_entry(key: str, fetcher, ttl_days: int = 30) -> CacheEntry:
    return CacheEntry(
        key=key, filename=f"{key}.zip", fetcher=fetcher, ttl_days=ttl_days
    )


@pytest.fixture()
def counting_fetcher():
    calls = []

    def fetcher(p: Path) -> None:
        calls.append(p)
        _make_valid_zip(p)

    return fetcher, calls


def test_ensure_serves_fresh_within_ttl(tmp_path: Path, counting_fetcher) -> None:
    """Within the TTL window, ensure returns the cached path without re-fetching."""
    fetcher, calls = counting_fetcher
    entry = _make_entry("crs", fetcher, ttl_days=30)

    with freeze_time("2024-01-01"):
        manager = CacheManager(base_dir=tmp_path)
        manager.ensure(entry)
        assert len(calls) == 1

    # Advance to day 20 (within TTL of 30 days)
    with freeze_time("2024-01-21"):
        manager2 = CacheManager(base_dir=tmp_path)
        manager2.ensure(entry)
        assert len(calls) == 1, "Should NOT re-fetch within TTL"


def test_ensure_refetches_after_ttl(tmp_path: Path, counting_fetcher) -> None:
    """After the TTL window, ensure re-fetches the data."""
    fetcher, calls = counting_fetcher
    entry = _make_entry("crs", fetcher, ttl_days=30)

    with freeze_time("2024-01-01"):
        manager = CacheManager(base_dir=tmp_path)
        manager.ensure(entry)
        assert len(calls) == 1

    # Advance past TTL (31 days later)
    with freeze_time("2024-02-01"):
        manager2 = CacheManager(base_dir=tmp_path)
        manager2.ensure(entry)
        assert len(calls) == 2, "Should re-fetch after TTL expires"
