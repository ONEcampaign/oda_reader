"""Tests for oda_reader top-level deprecation shims (Phase-2 fix #25)."""

import inspect
import sys
import types
import warnings

import pytest

# The oda_reader deprecation shims are gated on ``"oda_data" in sys.modules``
# so they stay silent for standalone oda_reader users.  In oda-importer's test
# suite oda_data is not installed, so we insert a synthetic placeholder to
# satisfy the gate without requiring the real package.
if "oda_data" not in sys.modules:
    sys.modules["oda_data"] = types.ModuleType("oda_data")

import oda_reader
from oda_reader._cache.config import get_cache_dir
from oda_reader.aiddata import download_aiddata
from oda_reader.download.download_tools import bulk_download_aiddata


@pytest.fixture(autouse=True)
def reset_shim_flags():
    """Ensure each test starts with fresh warned flags."""
    oda_reader._WARNED_SHIMS.clear()
    yield
    oda_reader._WARNED_SHIMS.clear()


def test_clear_cache_emits_deprecation_warning_once() -> None:
    """oda_reader.clear_cache emits exactly one DeprecationWarning across two calls."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        oda_reader.clear_cache()
        oda_reader.clear_cache()

    deprecations = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert "oda_data.cache.clear" in str(deprecations[0].message)


def test_set_cache_dir_emits_deprecation_warning_once(tmp_path) -> None:
    """oda_reader.set_cache_dir emits exactly one DeprecationWarning across two calls."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        oda_reader.set_cache_dir(str(tmp_path))
        oda_reader.set_cache_dir(str(tmp_path))

    deprecations = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert "oda_data.set_cache_root" in str(deprecations[0].message)


def test_enable_cache_emits_deprecation_warning_once() -> None:
    """oda_reader.enable_cache emits exactly one DeprecationWarning across two calls."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        oda_reader.enable_cache()
        oda_reader.enable_cache()

    deprecations = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert "oda_data.cache.enable_cache" in str(deprecations[0].message)


def test_disable_cache_emits_deprecation_warning_once() -> None:
    """oda_reader.disable_cache emits exactly one DeprecationWarning across two calls."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        oda_reader.disable_cache()
        oda_reader.disable_cache()

    deprecations = [x for x in w if issubclass(x.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert "oda_data.cache.disable_cache" in str(deprecations[0].message)


def test_aiddata_unaffected() -> None:
    """download_aiddata and bulk_download_aiddata must not have use_raw_cache (Phase-2 fix #18)."""
    assert "use_raw_cache" not in inspect.signature(download_aiddata).parameters
    assert "use_raw_cache" not in inspect.signature(bulk_download_aiddata).parameters


def test_shims_forward_correctly(tmp_path) -> None:
    """After the warning, the shims still forward to the underlying implementation."""
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        oda_reader.set_cache_dir(str(tmp_path))

    # set_cache_dir resolves symlinks (e.g. macOS /tmp -> /private/tmp), so
    # compare against the resolved form.
    assert get_cache_dir() == tmp_path.resolve()
