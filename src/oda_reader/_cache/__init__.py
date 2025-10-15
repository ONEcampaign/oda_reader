"""Cache management module for oda_reader.

This module provides a comprehensive caching system with multiple layers:
- HTTP caching (requests-cache) for API responses
- DataFrame caching for processed query results
- Bulk file caching for large downloads (CRS, Multisystem, AidData)

The module is organized into:
- config: Cache directory configuration and platform-aware paths
- manager: CacheManager for bulk file downloads (pydeflate-style)
- dataframe: DataFrame caching layer
- legacy: Backward-compatible utilities
"""

# Configuration
from oda_reader._cache.config import (
    get_cache_dir,
    set_cache_dir,
    reset_cache_dir,
    get_http_cache_path,
    get_bulk_cache_dir,
    get_dataframe_cache_dir,
)

# Bulk file cache manager
from oda_reader._cache.manager import (
    CacheEntry,
    CacheManager,
    bulk_cache_manager,
)

# DataFrame cache
from oda_reader._cache.dataframe import (
    DataFrameCache,
    dataframe_cache,
)

# Legacy functions (backward compatibility)
from oda_reader._cache.legacy import (
    memory,
    cache_dir,
    set_cache_dir as legacy_set_cache_dir,
    get_cache_size_mb,
    clear_cache,
    clear_old_cache_entries,
    enforce_cache_limits,
    cache_info,
    disable_cache,
    enable_cache,
    CACHE_MAX_SIZE_MB,
    CACHE_MAX_AGE_HOURS,
)

__all__ = [
    # Configuration
    "get_cache_dir",
    "set_cache_dir",
    "reset_cache_dir",
    "get_http_cache_path",
    "get_bulk_cache_dir",
    "get_dataframe_cache_dir",
    # Bulk cache manager
    "CacheEntry",
    "CacheManager",
    "bulk_cache_manager",
    # DataFrame cache
    "DataFrameCache",
    "dataframe_cache",
    # Legacy (backward compatibility)
    "memory",
    "cache_dir",
    "legacy_set_cache_dir",
    "get_cache_size_mb",
    "clear_cache",
    "clear_old_cache_entries",
    "enforce_cache_limits",
    "cache_info",
    "disable_cache",
    "enable_cache",
    "CACHE_MAX_SIZE_MB",
    "CACHE_MAX_AGE_HOURS",
]
