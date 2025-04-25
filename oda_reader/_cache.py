import os
import shutil
import time
from pathlib import Path

from joblib import Memory


# ----------  configuration & joblib ---------------------------------
_DEFAULT = Path(__file__).resolve().parent / ".cache"
_CACHE_DIR = Path(os.getenv("ODA_READER_CACHE_DIR", _DEFAULT)).expanduser()
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_memory_store = Memory(_CACHE_DIR, verbose=0)


def memory() -> Memory:
    """Return the current memory store."""
    return _memory_store


def set_cache_dir(path) -> None:
    """Move the cache; call before any cached function is executed."""
    global _CACHE_DIR, _memory_store
    new_path = Path(path).expanduser()
    new_path.mkdir(parents=True, exist_ok=True)

    _CACHE_DIR = new_path
    _memory_store.store_backend.location = str(new_path)


def cache_dir() -> Path:
    """Return the current cache directory (read‑only helper)."""
    return _CACHE_DIR


CACHE_MAX_SIZE_MB = 2_500
CACHE_MAX_AGE_HOURS = 168
_has_logged_cache_message = False


def _human_mb(byte_count: float) -> float:
    return byte_count / 1_048_576


def get_cache_size_mb(path: Path | None = None) -> float:
    path = path or _CACHE_DIR
    total = sum(
        os.path.getsize(fp)
        for dirpath, _, files in os.walk(path)
        for fp in (Path(dirpath) / f for f in files)
        if fp.exists()
    )
    return _human_mb(total)


def clear_cache(path: Path | None = None) -> None:
    import logging

    logger = logging.getLogger("oda_reader")
    path = path or _CACHE_DIR
    if path.exists():
        shutil.rmtree(path)
        logger.info("Cache directory cleared.")
    path.mkdir(parents=True, exist_ok=True)


def clear_old_cache_entries(
    path: Path | None = None, max_age_hours: int = CACHE_MAX_AGE_HOURS
) -> None:
    import logging

    logger = logging.getLogger("oda_reader")
    path = path or _CACHE_DIR
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
    import logging

    logger = logging.getLogger("oda_reader")

    path = path or _CACHE_DIR
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
    def wrapper(*args, **kwargs):
        import logging

        logger = logging.getLogger("oda_reader")
        if memory().store_backend and not _has_logged_cache_message:
            logger.info(
                f"""\n[oda-reader]  Caching is enabled (and lasts a maximum of 7 days)."""
            )
        globals()["_has_logged_cache_message"] = True
        return func(*args, **kwargs)

    return wrapper


def disable_cache() -> None:
    """
    Turn OFF disk caching globally.
    Existing cached results are still readable unless you clear the dir.
    """
    global _memory_store
    _memory_store.store_backend = None


def enable_cache() -> None:
    """
    Turn ON disk caching in the current cache directory.
    """
    global _memory_store
    _memory_store = Memory(_CACHE_DIR, verbose=0)
