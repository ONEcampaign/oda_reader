"""Cache configuration and directory management for oda_reader.

Directory resolution is delegated to ``readerkit.resolve_cache_dir``, which turns the
programmatic override, the ``ODA_READER_CACHE_DIR`` env var (or, family-wide,
``BBLOCKS_CACHE_DIR``), or a platform default into an absolute, schema- and
version-segmented cache root: ``<root>/v<schema>/oda-reader/<oda_reader version>``.
``app_version`` is deliberately the full package version, not a coarsened
major.minor — a stale cache surviving a release that changed parsing is worse than
a re-download, even at oda_reader's cache size.
"""

import socket
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path

from readerkit import resolve_cache_dir

_HOSTNAME = socket.gethostname()

_CACHE_DIR_OVERRIDE: Path | None = None

# Callbacks invoked when the cache root changes, so module-level singletons
# (CacheManager, DataFrameCache) can rebuild against the new path. Modules
# register on import via ``register_cache_dir_change_listener``.
_CACHE_DIR_LISTENERS: list[Callable[[], None]] = []


def get_cache_dir() -> Path:
    """Get the cache directory path.

    Resolution priority (see ``readerkit.resolve_cache_dir``):
    1. Programmatic override via set_cache_dir()
    2. Environment variable ODA_READER_CACHE_DIR
    3. BBLOCKS_CACHE_DIR (family-wide fallback)
    4. Platform default: platformdirs.user_cache_dir("readerkit", appauthor=False)

    The returned path is schema- and version-segmented for automatic
    invalidation on upgrades. ``ensure_exists=True``: all three real consumers
    (get_http_cache_path, get_bulk_cache_dir, get_dataframe_cache_dir) capture
    this path once behind a lazy singleton, so it is resolved at most a
    handful of times per process, not on every cache access — the cost of
    creating the directory and probing it for writability here is
    negligible. In exchange, a misconfigured or read-only cache root fails
    with a structured ``readerkit.CacheDirectoryError`` naming the exact
    unwritable path and distinguishing a read-only filesystem from a
    permissions problem, instead of a raw ``PermissionError`` surfacing later
    from whichever consumer happened to touch the filesystem first.

    Returns:
        Path: The cache directory path.
    """
    return resolve_cache_dir(
        app="oda-reader",
        app_version=version("oda_reader"),
        cache_dir=_CACHE_DIR_OVERRIDE,
        ensure_exists=True,
    )


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
