"""
This oda_reader package is a simple python wrapper for the OECD explorer API,
specifically designed to work with OECD DAC data.
"""

import sys
import warnings
from collections.abc import Callable
from typing import Any

from oda_reader._cache import (
    bulk_cache_manager,
    cache_dir,  # Deprecated alias
    dataframe_cache,
    enforce_cache_limits,
    get_cache_dir,
    reset_cache_dir,
)
from oda_reader._cache.config import set_cache_dir as _impl_set_cache_dir
from oda_reader._cache.legacy import clear_cache as _impl_clear_cache
from oda_reader._cache.legacy import disable_cache as _impl_disable_cache
from oda_reader._cache.legacy import enable_cache as _impl_enable_cache
from oda_reader.aiddata import download_aiddata
from oda_reader.common import (
    API_RATE_LIMITER,
    clear_http_cache,
    disable_http_cache,
    enable_http_cache,
    get_http_cache_info,
)
from oda_reader.crs import bulk_download_crs, download_crs, download_crs_file
from oda_reader.dac1 import download_dac1
from oda_reader.dac2a import bulk_download_dac2a, download_dac2a
from oda_reader.download.query_builder import QueryBuilder
from oda_reader.download.version_discovery import clear_version_cache
from oda_reader.exceptions import BulkDownloadHTTPError, BulkPayloadCorruptError
from oda_reader.multisystem import bulk_download_multisystem, download_multisystem
from oda_reader.tools import get_available_filters

# Each shim emits a one-time-per-session DeprecationWarning when oda_data is
# also imported (umbrella users should migrate to oda_data.cache.*); standalone
# oda_reader users see no warning.
_WARNED_SHIMS: set[str] = set()


def _warn_once_if_oda_data_imported(name: str, replacement: str) -> None:
    if name in _WARNED_SHIMS or "oda_data" not in sys.modules:
        return
    warnings.warn(
        f"oda_reader.{name} is deprecated for users who also import oda_data; "
        f"use {replacement} for the umbrella API. This shim is preserved for "
        "standalone oda_reader users through 1.x and removed in 2.0.",
        DeprecationWarning,
        stacklevel=3,
    )
    _WARNED_SHIMS.add(name)


def _make_deprecation_shim(
    name: str, replacement: str, impl: Callable[..., Any], one_liner: str
) -> Callable[..., Any]:
    def shim(*args: Any, **kwargs: Any) -> Any:
        _warn_once_if_oda_data_imported(name, replacement)
        return impl(*args, **kwargs)

    shim.__name__ = name
    shim.__qualname__ = name
    shim.__doc__ = (
        f"{one_liner} Deprecated under the oda_data umbrella; use {replacement}."
    )
    return shim


clear_cache = _make_deprecation_shim(
    "clear_cache",
    "oda_data.cache.clear('all')",
    _impl_clear_cache,
    "Clear the cache directory.",
)
set_cache_dir = _make_deprecation_shim(
    "set_cache_dir",
    "oda_data.set_cache_root() or the ODA_DATA_CACHE_DIR env var",
    _impl_set_cache_dir,
    "Set a custom cache directory path.",
)
enable_cache = _make_deprecation_shim(
    "enable_cache",
    "oda_data.cache.enable_cache('all')",
    _impl_enable_cache,
    "Enable caching globally.",
)
disable_cache = _make_deprecation_shim(
    "disable_cache",
    "oda_data.cache.disable_cache('all')",
    _impl_disable_cache,
    "Disable caching globally.",
)


__all__ = [
    # Boundary contract
    "BulkPayloadCorruptError",
    "BulkDownloadHTTPError",
    # Data download
    "QueryBuilder",
    "download_dac1",
    "download_dac2a",
    "bulk_download_dac2a",
    "download_multisystem",
    "bulk_download_multisystem",
    "download_crs",
    "bulk_download_crs",
    "download_crs_file",
    "download_aiddata",
    "get_available_filters",
    # Cache configuration
    "get_cache_dir",
    "set_cache_dir",
    "reset_cache_dir",
    # HTTP cache management
    "enable_http_cache",
    "disable_http_cache",
    "clear_http_cache",
    "get_http_cache_info",
    # Version discovery cache
    "clear_version_cache",
    # DataFrame and bulk cache managers
    "dataframe_cache",
    "bulk_cache_manager",
    # Rate limiting
    "API_RATE_LIMITER",
    # Legacy (backward compatibility - deprecated for oda_data users)
    "enable_cache",
    "disable_cache",
    "clear_cache",
    "enforce_cache_limits",
    "cache_dir",
]
