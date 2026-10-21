"""Tests for CacheManager's startup deletion of expired entries."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from oda_reader._cache.manager import CacheManager

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def _populate(base_dir: Path, ages_days: dict[str, int], ttl_days: int = 90) -> None:
    """Write a manifest entry and a file for each key, downloaded that many days ago."""
    now = datetime.now(UTC)
    manifest = {}
    for key, age in ages_days.items():
        (base_dir / f"{key}.zip").write_bytes(b"PK")
        manifest[key] = {
            "filename": f"{key}.zip",
            "downloaded_at": (now - timedelta(days=age)).strftime(ISO_FORMAT),
            "ttl_days": ttl_days,
            "version": None,
        }
    (base_dir / "manifest.json").write_text(json.dumps(manifest))


def _manifest_keys(base_dir: Path) -> set[str]:
    return set(json.loads((base_dir / "manifest.json").read_text()))


def test_keeps_every_entry_within_ttl(tmp_path: Path) -> None:
    """Four datasets in use must all stay cached, whatever their order."""
    _populate(tmp_path, {"crs": 60, "dac1": 30, "dac2a": 10, "multi": 1})

    CacheManager(base_dir=tmp_path)

    assert {p.stem for p in tmp_path.glob("*.zip")} == {"crs", "dac1", "dac2a", "multi"}
    assert _manifest_keys(tmp_path) == {"crs", "dac1", "dac2a", "multi"}


def test_removes_entries_past_their_ttl(tmp_path: Path) -> None:
    _populate(tmp_path, {"old": 91, "fresh": 89})

    CacheManager(base_dir=tmp_path)

    assert {p.stem for p in tmp_path.glob("*.zip")} == {"fresh"}
    assert _manifest_keys(tmp_path) == {"fresh"}


def test_unlink_failure_keeps_record_for_retry(tmp_path: Path) -> None:
    _populate(tmp_path, {"old": 120})
    original_unlink = Path.unlink

    def raising_unlink(self, missing_ok=False):
        if self.suffix == ".zip":
            raise OSError("simulated failure")
        return original_unlink(self, missing_ok=missing_ok)

    with patch.object(Path, "unlink", raising_unlink):
        CacheManager(base_dir=tmp_path)
    assert _manifest_keys(tmp_path) == {"old"}

    CacheManager(base_dir=tmp_path)
    assert _manifest_keys(tmp_path) == set()
    assert not (tmp_path / "old.zip").exists()
