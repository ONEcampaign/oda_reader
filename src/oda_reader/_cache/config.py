"""Cache configuration and directory management for oda_reader.

This module provides platform-aware cache directory resolution following XDG Base
Directory specifications on Linux and platform conventions elsewhere.
"""

import os
from pathlib import Path
from typing import Optional

from platformdirs import user_cache_dir

# Version for cache versioning (hardcoded to avoid circular import)
# This should match the version in __init__.py
__version__ = "1.3.0"

# Global override for cache directory (set via set_cache_dir)
_CACHE_DIR_OVERRIDE: Optional[Path] = None


def get_cache_dir() -> Path:
    """Get the cache directory path.

    Resolution priority:
    1. Programmatic override via set_cache_dir()
    2. Environment variable ODA_READER_CACHE_DIR
    3. Platform default via platformdirs

    The cache includes version in path for automatic invalidation on upgrades.

    Returns:
        Path: The cache directory path.
    """
    # Priority 1: Programmatic override
    if _CACHE_DIR_OVERRIDE is not None:
        return _CACHE_DIR_OVERRIDE

    # Priority 2: Environment variable
    if env_dir := os.getenv("ODA_READER_CACHE_DIR"):
        return Path(env_dir).expanduser().resolve()

    # Priority 3: Platform default with version
    # This ensures cache is invalidated on package upgrades
    base = Path(user_cache_dir("oda-reader", "oda-reader"))
    return base / __version__


def set_cache_dir(path: str | Path) -> None:
    """Set a custom cache directory path.

    This takes precedence over environment variables and platform defaults.
    Changes affect all future cache operations.

    Args:
        path: The directory path to use for caching.

    Example:
        >>> from oda_reader import set_cache_dir
        >>> set_cache_dir("/tmp/my_cache")
    """
    global _CACHE_DIR_OVERRIDE
    _CACHE_DIR_OVERRIDE = Path(path).expanduser().resolve()


def reset_cache_dir() -> None:
    """Reset cache directory to default (remove override).

    After calling this, cache directory will be determined by environment
    variable or platform default.
    """
    global _CACHE_DIR_OVERRIDE
    _CACHE_DIR_OVERRIDE = None


def get_http_cache_path() -> Path:
    """Get the path for HTTP response cache (requests-cache SQLite file).

    Returns:
        Path: Path to the HTTP cache database file.
    """
    cache_dir = get_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "http_cache.sqlite"


def get_bulk_cache_dir() -> Path:
    """Get the directory for bulk file downloads (parquet files).

    Returns:
        Path: Path to the bulk files cache directory.
    """
    cache_dir = get_cache_dir()
    bulk_dir = cache_dir / "bulk_files"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    return bulk_dir


def get_dataframe_cache_dir() -> Path:
    """Get the directory for cached processed DataFrames.

    Returns:
        Path: Path to the DataFrame cache directory.
    """
    cache_dir = get_cache_dir()
    df_dir = cache_dir / "dataframes"
    df_dir.mkdir(parents=True, exist_ok=True)
    return df_dir
