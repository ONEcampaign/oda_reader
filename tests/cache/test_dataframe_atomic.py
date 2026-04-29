"""Tests for atomic DataFrameCache.set writes."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd

import oda_reader._cache.dataframe as df_module
from oda_reader._cache.dataframe import DataFrameCache


def test_set_is_atomic(tmp_path: Path, caplog) -> None:
    """A failed write must not leave a partial file at the destination, must
    not leak a tmp sibling, and must not propagate as an exception (cache
    writes are best-effort)."""
    cache = DataFrameCache(cache_dir=tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3]})

    with patch.object(pd.DataFrame, "to_parquet", side_effect=OSError("disk error")):
        cache.set(
            df,
            dataflow_id="DSD_CRS@DF_CRS",
            dataflow_version="1.0",
            url="http://example.com",
            pre_process=True,
            dotstat_codes=True,
        )

    assert list(tmp_path.glob("*.parquet")) == []
    assert list(tmp_path.glob("*.tmp-*")) == []
    assert any("Failed to cache DataFrame" in r.message for r in caplog.records)


def test_set_no_tmp_file_left_on_success(tmp_path: Path) -> None:
    """DataFrameCache.set leaves no *.tmp-* sibling after a successful write."""
    cache = DataFrameCache(cache_dir=tmp_path)
    df = pd.DataFrame({"a": [1, 2, 3]})

    cache.set(
        df,
        dataflow_id="DSD_CRS@DF_CRS",
        dataflow_version="1.0",
        url="http://example.com",
        pre_process=True,
        dotstat_codes=True,
    )

    tmp_files = list(tmp_path.glob("*.tmp-*"))
    assert tmp_files == [], f"Stale tmp files found: {tmp_files}"

    parquet_files = list(tmp_path.glob("*.parquet"))
    assert len(parquet_files) == 1


def test_set_uses_host_pid_suffix(tmp_path: Path, monkeypatch) -> None:
    """DataFrameCache.set temp file uses host+pid suffix."""
    recorded_tmp: list[Path] = []
    original_to_parquet = pd.DataFrame.to_parquet

    def capturing_to_parquet(self, path, *args, **kwargs):
        recorded_tmp.append(Path(path))
        return original_to_parquet(self, path, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", capturing_to_parquet)
    monkeypatch.setattr(df_module, "_HOSTNAME", "testhost")

    cache = DataFrameCache(cache_dir=tmp_path)
    df = pd.DataFrame({"x": [1]})

    cache.set(
        df,
        dataflow_id="DSD",
        dataflow_version="1",
        url="http://x.com",
        pre_process=False,
        dotstat_codes=False,
    )

    assert len(recorded_tmp) == 1
    tmp_name = recorded_tmp[0].name
    assert "tmp-testhost-" in tmp_name
