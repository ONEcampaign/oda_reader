"""Tests for CacheManager's startup deletion of expired entries."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from oda_reader._cache.manager import CacheManager

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
_FLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_CRS@DF_CRS/"


def _populate(
    base_dir: Path,
    ages_days: dict[str, int],
    *,
    labels: dict[str, str] | None = None,
    ttl_days: int = 90,
) -> None:
    """Write a manifest entry and a file for each key, downloaded that many days ago.

    A key in ``labels`` records ``_FLOW_URL`` and that label as its dataset.
    """
    labels = labels or {}
    now = datetime.now(UTC)
    manifest = {}
    for key, age in ages_days.items():
        (base_dir / f"{key}.zip").write_bytes(b"PK")
        manifest[key] = {
            "filename": f"{key}.zip",
            "downloaded_at": (now - timedelta(days=age)).strftime(ISO_FORMAT),
            "ttl_days": ttl_days,
            "version": None,
            "source": "download",
            "flow_url": _FLOW_URL if key in labels else None,
            "label": labels.get(key),
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


@pytest.mark.parametrize(
    ("labels", "kept"),
    [
        ({"old": "CRS", "fresh": "CRS"}, {"fresh"}),
        ({"old": "CRS", "fresh": "DAC1"}, {"old", "fresh"}),
        ({}, {"old", "fresh"}),
    ],
    ids=["superseded", "only-copy-of-dataset", "no-dataset"],
)
def test_removes_expired_entry_only_when_superseded(
    tmp_path: Path, labels: dict[str, str], kept: set[str]
) -> None:
    _populate(tmp_path, {"old": 91, "fresh": 89}, labels=labels)

    CacheManager(base_dir=tmp_path)

    assert {p.stem for p in tmp_path.glob("*.zip")} == kept
    assert _manifest_keys(tmp_path) == kept


def test_newer_record_without_file_does_not_supersede(tmp_path: Path) -> None:
    _populate(tmp_path, {"old": 120, "fresh": 1}, labels={"old": "CRS", "fresh": "CRS"})
    (tmp_path / "fresh.zip").unlink()

    CacheManager(base_dir=tmp_path)

    assert (tmp_path / "old.zip").exists()


def test_unlink_failure_keeps_record_for_retry(tmp_path: Path) -> None:
    _populate(tmp_path, {"old": 120, "fresh": 1}, labels={"old": "CRS", "fresh": "CRS"})
    original_unlink = Path.unlink

    def raising_unlink(self, missing_ok=False):
        if self.suffix == ".zip":
            raise OSError("simulated failure")
        return original_unlink(self, missing_ok=missing_ok)

    with patch.object(Path, "unlink", raising_unlink):
        CacheManager(base_dir=tmp_path)
    assert _manifest_keys(tmp_path) == {"old", "fresh"}

    CacheManager(base_dir=tmp_path)
    assert _manifest_keys(tmp_path) == {"fresh"}
    assert not (tmp_path / "old.zip").exists()
