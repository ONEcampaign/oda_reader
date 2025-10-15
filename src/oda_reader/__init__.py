"""
This oda_reader package is a simple python wrapper for the OECD explorer API,
specifically designed to work with OECD DAC data.
"""

# Core data download functions
from oda_reader.download.query_builder import QueryBuilder
from oda_reader.dac1 import download_dac1
from oda_reader.dac2a import download_dac2a
from oda_reader.multisystem import download_multisystem, bulk_download_multisystem
from oda_reader.crs import download_crs, bulk_download_crs, download_crs_file
from oda_reader.aiddata import download_aiddata
from oda_reader.tools import get_available_filters

# Cache management (new system)
from oda_reader._cache import (
    get_cache_dir,
    set_cache_dir,
    reset_cache_dir,
    dataframe_cache,
    bulk_cache_manager,
    # Legacy functions (for backward compatibility)
    enable_cache,
    disable_cache,
    clear_cache,
    enforce_cache_limits,
    cache_dir,  # Deprecated alias
)
from oda_reader.common import (
    API_RATE_LIMITER,
    enable_http_cache,
    disable_http_cache,
    clear_http_cache,
    get_http_cache_info,
)


__all__ = [
    # Data download
    "QueryBuilder",
    "download_dac1",
    "download_dac2a",
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
    # DataFrame and bulk cache managers
    "dataframe_cache",
    "bulk_cache_manager",
    # Rate limiting
    "API_RATE_LIMITER",
    # Legacy (backward compatibility)
    "enable_cache",
    "disable_cache",
    "clear_cache",
    "enforce_cache_limits",
    "cache_dir",
]
