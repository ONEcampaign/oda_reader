"""Legacy cache utilities (mostly deprecated).

This module is kept for backward compatibility. New code should use:
- oda_reader.cache_config for cache directory configuration
- oda_reader.common for HTTP caching (requests-cache)
- oda_reader.dataframe_cache for DataFrame caching
- oda_reader.cache_manager for bulk file caching
"""

import logging
import os
import shutil
import time
from pathlib import Path
from typing import Optional

from oda_reader._cache.config import get_cache_dir

logger = logging.getLogger("oda_reader")

# Constants
CACHE_MAX_SIZE_MB = 2_500
CACHE_MAX_AGE_HOURS = 168
_has_logged_cache_message = False

# Legacy joblib support (deprecated)
_JOBLIB_MEMORY: Optional[object] = None


def memory():
    """Return a dummy memory store (deprecated).

    This function is kept for backward compatibility but no longer uses joblib.
    The caching system has been refactored to use requests-cache and file-based caching.

    Returns:
        Object with store_backend=None to indicate caching is handled elsewhere.
    """
    global _JOBLIB_MEMORY
    if _JOBLIB_MEMORY is None:
        # Create a dummy object that has store_backend attribute
        class DummyMemory:
            store_backend = None

        _JOBLIB_MEMORY = DummyMemory()
    return _JOBLIB_MEMORY


def cache_dir() -> Path:
    """Return the current cache directory.

    Deprecated: Use oda_reader.cache_config.get_cache_dir() instead.
    """
    return get_cache_dir()


def set_cache_dir(path) -> None:
    """Set a custom cache directory path (deprecated).

    Use oda_reader._cache.config.set_cache_dir() instead.
    """
    from oda_reader._cache.config import set_cache_dir as new_set_cache_dir

    new_set_cache_dir(path)


def _human_mb(byte_count: float) -> float:
    """Convert bytes to megabytes."""
    return byte_count / 1_048_576


def get_cache_size_mb(path: Path | None = None) -> float:
    """Get the size of the cache directory in MB.

    Args:
        path: Path to check. If None, uses the default cache directory.

    Returns:
        float: Size in megabytes
    """
    path = path or get_cache_dir()
    total = sum(
        os.path.getsize(fp)
        for dirpath, _, files in os.walk(path)
        for fp in (Path(dirpath) / f for f in files)
        if fp.exists()
    )
    return _human_mb(total)


def clear_cache(path: Path | None = None) -> None:
    """Clear the cache directory.

    Args:
        path: Path to clear. If None, clears the default cache directory.
    """
    path = path or get_cache_dir()
    if path.exists():
        shutil.rmtree(path)
        logger.info("Cache directory cleared.")
    path.mkdir(parents=True, exist_ok=True)


def clear_old_cache_entries(
    path: Path | None = None, max_age_hours: int = CACHE_MAX_AGE_HOURS
) -> None:
    """Clear cache entries older than max_age_hours.

    Args:
        path: Path to check. If None, uses the default cache directory.
        max_age_hours: Maximum age in hours before deletion.
    """
    path = path or get_cache_dir()
    cutoff = time.time() - max_age_hours * 3600
    for dirpath, _, files in os.walk(path):
        for f in files:
            fp = Path(dirpath) / f
            if fp.exists() and fp.stat().st_mtime < cutoff:
                fp.unlink(missing_ok=True)
                logger.info(f"Deleted old cache file: {fp}")


def enforce_cache_limits(
    path: Path | None = None,
    max_size_mb: float = CACHE_MAX_SIZE_MB,
    max_age_hours: int = CACHE_MAX_AGE_HOURS,
) -> None:
    """Enforce cache size and age limits.

    This function is now called lazily on first cache use, not at import time.

    Args:
        path: Path to enforce limits on. If None, uses the default cache directory.
        max_size_mb: Maximum cache size in megabytes.
        max_age_hours: Maximum age of cache entries in hours.
    """
    path = path or get_cache_dir()
    clear_old_cache_entries(path, max_age_hours)
    size_mb = get_cache_size_mb(path)
    if size_mb <= max_size_mb:
        return

    logger.warning(f"Cache size {size_mb:.1f} MB exceeds limit of {max_size_mb} MB.")
    logger.info("Deleting oldest cache files to reduce size...")

    # Collect all files with their modified times
    file_info = []
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                mtime = os.path.getmtime(fp)
                size = os.path.getsize(fp)
                file_info.append((fp, mtime, size))
            except FileNotFoundError:
                continue

    # Sort by modification time (oldest first)
    file_info.sort(key=lambda x: x[1])

    # Remove oldest files until under limit
    removed = 0
    for fp, _, size in file_info:
        try:
            os.remove(fp)
            removed += size
            size_mb -= size / 1_048_576
            if size_mb <= max_size_mb:
                break
        except FileNotFoundError:
            continue

    logger.info(
        f"Removed {removed / 1_048_576:.1f}MB of cache files to enforce size limit."
    )


def cache_info(func):
    """Decorator that logs cache info (deprecated).

    This decorator is kept for backward compatibility with existing code.
    """

    def wrapper(*args, **kwargs):
        global _has_logged_cache_message
        if not _has_logged_cache_message:
            logger.info("[oda-reader] Caching is enabled.")
            _has_logged_cache_message = True
        return func(*args, **kwargs)

    return wrapper


def disable_cache() -> None:
    """Disable caching globally.

    This disables HTTP caching and DataFrame caching.
    """
    from oda_reader.common import disable_http_cache
    from oda_reader._cache.dataframe import dataframe_cache

    disable_http_cache()
    dataframe_cache().disable()
    logger.info("Caching disabled globally.")


def enable_cache() -> None:
    """Enable caching globally.

    This enables HTTP caching and DataFrame caching.
    """
    from oda_reader.common import enable_http_cache
    from oda_reader._cache.dataframe import dataframe_cache

    enable_http_cache()
    dataframe_cache().enable()
    logger.info("Caching enabled globally.")
