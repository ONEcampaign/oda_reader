"""Tests for CacheManager LRU eviction on __init__."""

import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from oda_reader._cache.manager import CacheManager

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def _make_valid_zip(target: Path) -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("data.parquet", b"PAR1" + b"\x00" * 100)
    target.write_bytes(buf.getvalue())


def _write_manifest(base_dir: Path, entries: dict) -> None:
    """Write a manifest.json with the given entries dict."""
    manifest_path = base_dir / "manifest.json"
    with manifest_path.open("w") as f:
        json.dump(entries, f, indent=2)


def _make_manifest_entry(filename: str, downloaded_at: str) -> dict:
    return {
        "filename": filename,
        "downloaded_at": downloaded_at,
        "ttl_days": 30,
        "version": None,
    }


def _ts(year: int, month: int, day: int) -> str:
    return datetime(year, month, day, tzinfo=timezone.utc).strftime(ISO_FORMAT)


def test_keeps_n_most_recent(tmp_path: Path) -> None:
    """Pre-populate manifest with 4 entries; CacheManager(keep_n=2) evicts 2 oldest."""
    base = tmp_path / "cache"
    base.mkdir()

    entries = {
        "key_a": _make_manifest_entry("key_a.zip", _ts(2024, 1, 1)),
        "key_b": _make_manifest_entry("key_b.zip", _ts(2024, 2, 1)),
        "key_c": _make_manifest_entry("key_c.zip", _ts(2024, 3, 1)),
        "key_d": _make_manifest_entry("key_d.zip", _ts(2024, 4, 1)),
    }
    _write_manifest(base, entries)

    # Write the actual zip files so unlink can succeed.
    for key in entries:
        _make_valid_zip(base / entries[key]["filename"])

    CacheManager(base_dir=base, keep_n=2)

    # Should keep the 2 most recent (key_c, key_d); evict key_a and key_b.
    remaining = list((base).glob("*.zip"))
    remaining_names = {p.name for p in remaining}

    assert "key_c.zip" in remaining_names
    assert "key_d.zip" in remaining_names
    assert "key_a.zip" not in remaining_names
    assert "key_b.zip" not in remaining_names


def test_no_eviction_when_within_limit(tmp_path: Path) -> None:
    """With 2 entries and keep_n=2, no eviction occurs."""
    base = tmp_path / "cache"
    base.mkdir()

    entries = {
        "key_a": _make_manifest_entry("key_a.zip", _ts(2024, 1, 1)),
        "key_b": _make_manifest_entry("key_b.zip", _ts(2024, 2, 1)),
    }
    _write_manifest(base, entries)
    for key in entries:
        _make_valid_zip(base / entries[key]["filename"])

    CacheManager(base_dir=base, keep_n=2)

    remaining = {p.name for p in base.glob("*.zip")}
    assert remaining == {"key_a.zip", "key_b.zip"}


def test_unlink_failure_logs_but_does_not_raise(tmp_path: Path) -> None:
    """LRU eviction: unlink failure logs a warning but does not propagate."""
    base = tmp_path / "cache"
    base.mkdir()

    entries = {
        "key_a": _make_manifest_entry("key_a.zip", _ts(2024, 1, 1)),
        "key_b": _make_manifest_entry("key_b.zip", _ts(2024, 2, 1)),
        "key_c": _make_manifest_entry("key_c.zip", _ts(2024, 3, 1)),
    }
    _write_manifest(base, entries)
    # Do not write the actual files — unlink(missing_ok=True) should be silent.
    # We patch Path.unlink to raise OSError.
    original_unlink = Path.unlink

    def raising_unlink(self, missing_ok=False):
        if self.name.endswith(".zip"):
            raise OSError("simulated failure")
        return original_unlink(self, missing_ok=missing_ok)

    with patch.object(Path, "unlink", raising_unlink):
        # Should not raise despite unlink errors.
        CacheManager(base_dir=base, keep_n=2)
