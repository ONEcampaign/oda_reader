"""
This oda_reader package is a simple python wrapper for the OECD explorer API,
specifically designed to work with OECD DAC data.
"""

__version__ = "1.2.1"

from oda_reader._cache import (
    enforce_cache_limits,
    enable_cache,
    disable_cache,
    clear_cache,
    set_cache_dir,
    cache_dir,
)
from oda_reader.download.query_builder import QueryBuilder
from oda_reader.dac1 import download_dac1
from oda_reader.dac2a import download_dac2a
from oda_reader.multisystem import download_multisystem, bulk_download_multisystem
from oda_reader.crs import download_crs, bulk_download_crs, download_crs_file
from oda_reader.aiddata import download_aiddata
from oda_reader.tools import get_available_filters
from oda_reader.common import API_RATE_LIMITER

enforce_cache_limits()


__all__ = [
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
    "enforce_cache_limits",
    "enable_cache",
    "disable_cache",
    "clear_cache",
    "set_cache_dir",
    "cache_dir",
    "API_RATE_LIMITER",
]
