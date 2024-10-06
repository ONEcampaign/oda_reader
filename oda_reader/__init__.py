"""
This oda_reader package is a simple python wrapper for the OECD explorer API,
specifically designed to work with OECD DAC data.
"""

__version__ = "1.0.0"

from oda_reader.download.query_builder import QueryBuilder
from oda_reader.dac1 import download_dac1
from oda_reader.dac2a import download_dac2a
from oda_reader.multisystem import download_multisystem, bulk_download_multisystem
from oda_reader.crs import download_crs, bulk_download_crs, download_crs_file
from oda_reader.tools import get_available_filters


__all__ = [
    "QueryBuilder",
    "download_dac1",
    "download_dac2a",
    "download_multisystem",
    "bulk_download_multisystem",
    "download_crs",
    "bulk_download_crs",
    "download_crs_file",
    "get_available_filters",
]
