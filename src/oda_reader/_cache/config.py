"""Cache configuration and directory management for oda_reader.

This module provides platform-aware cache directory resolution following XDG Base
Directory specifications on Linux and platform conventions elsewhere.
"""

import os
import socket
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path

from platformdirs import user_cache_dir

# Resolved at import time so the cache path tracks the installed package version.
_CACHE_VERSION = version("oda_reader")

_HOSTNAME = socket.gethostname()

_CACHE_DIR_OVERRIDE: Path | None = None

# Callbacks invoked when the cache root changes, so module-level singletons
# (CacheManager, DataFrameCache) can rebuild against the new path. Modules
# register on import via ``register_cache_dir_change_listener``.
_CACHE_DIR_LISTENERS: list[Callable[[], None]] = []


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
    if _CACHE_DIR_OVERRIDE is not None:
        return _CACHE_DIR_OVERRIDE

    if env_dir := os.getenv("ODA_READER_CACHE_DIR"):
        return Path(env_dir).expanduser().resolve()

    base = Path(user_cache_dir("oda-reader", "oda-reader"))
    return base / _CACHE_VERSION


def set_cache_dir(path: str | Path) -> None:
    """Set a custom cache directory path.

    This takes precedence over environment variables and platform defaults.
    Changes affect all future cache operations and reset any module-level
    cache singletons so they pick up the new directory.

    Args:
        path: The directory path to use for caching.

    Example:
        >>> from oda_reader import set_cache_dir
        >>> set_cache_dir("/tmp/my_cache")
    """
    global _CACHE_DIR_OVERRIDE
    _CACHE_DIR_OVERRIDE = Path(path).expanduser().resolve()
    _notify_cache_dir_changed()


def reset_cache_dir() -> None:
    """Reset cache directory to default (remove override).

    After calling this, cache directory will be determined by environment
    variable or platform default. Resets module-level cache singletons.
    """
    global _CACHE_DIR_OVERRIDE
    _CACHE_DIR_OVERRIDE = None
    _notify_cache_dir_changed()


def register_cache_dir_change_listener(callback: Callable[[], None]) -> None:
    """Register a callback to fire when the cache directory changes.

    Args:
        callback: Zero-argument callable invoked after set_cache_dir or
            reset_cache_dir mutates the override. Used by cache singletons
            to rebuild against the new directory.
    """
    _CACHE_DIR_LISTENERS.append(callback)


def _notify_cache_dir_changed() -> None:
    for callback in _CACHE_DIR_LISTENERS:
        callback()


def get_http_cache_path() -> Path:
    """Get the path for HTTP response cache (requests-cache filesystem directory).

    Returns:
        Path: Path to the HTTP cache directory.
    """
    cache_dir = get_cache_dir()
    http_cache_dir = cache_dir / "http_cache"
    http_cache_dir.mkdir(parents=True, exist_ok=True)
    return http_cache_dir


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
