import os
import shutil
import time
from pathlib import Path


from oda_reader.common import logger, CACHE_DIR

CACHE_MAX_SIZE_MB = 2500
CACHE_MAX_AGE_HOURS = 168


def get_cache_size_mb(path: Path = CACHE_DIR) -> float:
    """Return total size of cache directory in megabytes."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp):
                total += os.path.getsize(fp)
    return total / 1_048_576  # bytes to MB


def clear_cache(path: Path = CACHE_DIR):
    """Clear all files from the cache directory."""
    if path.exists():
        shutil.rmtree(path)
        logger.info("Cache directory cleared.")
    path.mkdir(exist_ok=True)


def clear_old_cache_entries(
    path: Path = CACHE_DIR, max_age_hours: int = CACHE_MAX_AGE_HOURS
):
    """Delete cache files older than the max_age_hours."""
    cutoff = time.time() - (max_age_hours * 3600)
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.exists(fp) and os.path.getmtime(fp) < cutoff:
                os.remove(fp)
                logger.info(
                    f"Deleted old cache file: {fp} (cache expires after {max_age_hours} hours)"
                )


def enforce_cache_limits(
    path: Path = CACHE_DIR,
    max_size_mb: float = CACHE_MAX_SIZE_MB,
    max_age_hours: int = CACHE_MAX_AGE_HOURS,
):
    """Clear old cache entries, and if size still exceeds limit, delete oldest files until under limit."""
    clear_old_cache_entries(path, max_age_hours=max_age_hours)

    size_mb = get_cache_size_mb(path)
    if size_mb <= max_size_mb:
        return

    logger.warning(f"Cache size {size_mb:.1f}MB exceeds limit of {max_size_mb}MB.")
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
