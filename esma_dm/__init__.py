"""
ESMA Data Manager (esma-dm)
===========================

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) 
published reference data and transparency information.

This package provides modular access to:
- FIRDS: Financial Instruments Reference Data System
- FITRS: Financial Instruments Transparency System  
- Benchmarks: Benchmark data and regulations
- SSR: Short Selling Regulation data

Example:
    >>> from esma_dm import FIRDSClient, FITRSClient
    >>> 
    >>> # Access FIRDS reference data
    >>> firds = FIRDSClient()
    >>> equity_data = firds.get_latest_full_files(asset_type='E')
    >>> 
    >>> # Access FITRS transparency data
    >>> fitrs = FITRSClient()
    >>> transparency = fitrs.get_latest_full_files(asset_type='E')
"""

__version__ = "0.1.0"
__author__ = "Robin"
__description__ = "ESMA Data Manager - Comprehensive wrapper for ESMA published data"

from .firds import FIRDSClient
from .fitrs import FITRSClient
from .benchmarks import BenchmarksClient
from .ssr import SSRClient
from .config import Config

__all__ = [
    "FIRDSClient",
    "FITRSClient", 
    "BenchmarksClient",
    "SSRClient",
    "Config",
]
