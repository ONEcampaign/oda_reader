"""DataFrame caching layer for processed query results.

This module provides a second-tier cache on top of HTTP caching. It caches
the final processed DataFrames with cache keys that include preprocessing
parameters (pre_process, dotstat_codes), solving the correctness issue where
different preprocessing options would return cached data incorrectly.

Architecture:
- HTTP cache (requests-cache): Caches raw API responses
- DataFrame cache (this module): Caches processed DataFrames

This two-tier approach prevents cache explosion while maintaining correctness.
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from oda_reader._cache.config import get_dataframe_cache_dir

logger = logging.getLogger("oda_reader")


def _make_cache_key(
    dataflow_id: str,
    dataflow_version: str,
    url: str,
    pre_process: bool,
    dotstat_codes: bool,
    **kwargs,
) -> str:
    """Generate a deterministic cache key for a DataFrame query.

    The key includes all parameters that affect the final DataFrame:
    - dataflow_id and version
    - URL (includes filters and time period)
    - pre_process flag
    - dotstat_codes flag
    - Any other kwargs

    Args:
        dataflow_id: The dataflow identifier
        dataflow_version: The dataflow version
        url: The API URL (includes filters)
        pre_process: Whether preprocessing was applied
        dotstat_codes: Whether dotstat code conversion was applied
        **kwargs: Additional parameters

    Returns:
        str: A hash-based cache key
    """
    # Build a dict of all parameters that affect the result
    params = {
        "dataflow_id": dataflow_id,
        "dataflow_version": dataflow_version,
        "url": url,
        "pre_process": pre_process,
        "dotstat_codes": dotstat_codes,
        **kwargs,
    }

    # Create a deterministic JSON string (sorted keys)
    params_json = json.dumps(params, sort_keys=True)

    # Hash it to create a short key
    hash_obj = hashlib.sha256(params_json.encode())
    return hash_obj.hexdigest()[:16]  # First 16 chars of hash


class DataFrameCache:
    """Simple file-based cache for processed DataFrames.

    Uses parquet files for efficient storage and fast loading.
    """

    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize DataFrame cache.

        Args:
            cache_dir: Directory for cache storage. If None, uses get_dataframe_cache_dir().
        """
        self.cache_dir = cache_dir or get_dataframe_cache_dir()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._enabled = True

    def get(
        self,
        dataflow_id: str,
        dataflow_version: str,
        url: str,
        pre_process: bool,
        dotstat_codes: bool,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        """Get a cached DataFrame if it exists.

        Args:
            dataflow_id: The dataflow identifier
            dataflow_version: The dataflow version
            url: The API URL
            pre_process: Whether preprocessing was applied
            dotstat_codes: Whether dotstat code conversion was applied
            **kwargs: Additional parameters

        Returns:
            DataFrame if cached, None otherwise
        """
        if not self._enabled:
            return None

        cache_key = _make_cache_key(
            dataflow_id=dataflow_id,
            dataflow_version=dataflow_version,
            url=url,
            pre_process=pre_process,
            dotstat_codes=dotstat_codes,
            **kwargs,
        )

        cache_file = self.cache_dir / f"{cache_key}.parquet"

        if cache_file.exists():
            try:
                logger.info(f"Loading DataFrame from cache (key: {cache_key})")
                return pd.read_parquet(cache_file)
            except Exception as e:
                logger.warning(f"Failed to load cached DataFrame: {e}")
                # If corrupted, delete it
                cache_file.unlink(missing_ok=True)
                return None

        return None

    def set(
        self,
        df: pd.DataFrame,
        dataflow_id: str,
        dataflow_version: str,
        url: str,
        pre_process: bool,
        dotstat_codes: bool,
        **kwargs,
    ) -> None:
        """Cache a processed DataFrame.

        Args:
            df: The DataFrame to cache
            dataflow_id: The dataflow identifier
            dataflow_version: The dataflow version
            url: The API URL
            pre_process: Whether preprocessing was applied
            dotstat_codes: Whether dotstat code conversion was applied
            **kwargs: Additional parameters
        """
        if not self._enabled:
            return

        cache_key = _make_cache_key(
            dataflow_id=dataflow_id,
            dataflow_version=dataflow_version,
            url=url,
            pre_process=pre_process,
            dotstat_codes=dotstat_codes,
            **kwargs,
        )

        cache_file = self.cache_dir / f"{cache_key}.parquet"

        try:
            df.to_parquet(cache_file)
            logger.info(f"Cached DataFrame (key: {cache_key})")
        except Exception as e:
            logger.warning(f"Failed to cache DataFrame: {e}")

    def clear(self) -> None:
        """Clear all cached DataFrames."""
        for cache_file in self.cache_dir.glob("*.parquet"):
            cache_file.unlink(missing_ok=True)
        logger.info("DataFrame cache cleared")

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with total_entries and total_size_mb
        """
        cache_files = list(self.cache_dir.glob("*.parquet"))
        total_size = sum(f.stat().st_size for f in cache_files if f.exists())

        return {
            "total_entries": len(cache_files),
            "total_size_mb": total_size / (1024 * 1024),
        }

    def enable(self) -> None:
        """Enable DataFrame caching."""
        self._enabled = True

    def disable(self) -> None:
        """Disable DataFrame caching."""
        self._enabled = False


# Global singleton
_DATAFRAME_CACHE: Optional[DataFrameCache] = None


def dataframe_cache() -> DataFrameCache:
    """Get the global DataFrame cache singleton.

    Returns:
        DataFrameCache: The global cache instance
    """
    global _DATAFRAME_CACHE
    if _DATAFRAME_CACHE is None:
        _DATAFRAME_CACHE = DataFrameCache()
    return _DATAFRAME_CACHE
