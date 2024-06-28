"""
This oda_reader package is a simple python wrapper for the OECD explorer API,
specifically designed to work with OECD DAC data.
"""

__version__ = "0.2.2"

from oda_reader.download.query_builder import QueryBuilder
from oda_reader.dac1 import download_dac1
from oda_reader.dac2a import download_dac2a


__all__ = ["QueryBuilder", "download_dac1", "download_dac2a"]
