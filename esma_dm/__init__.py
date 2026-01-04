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
    >>> from esma_dm import FIRDSClient
    >>> 
    >>> # Initialize client with DuckDB storage (default)
    >>> firds = FIRDSClient()
    >>> 
    >>> # Download and index latest files
    >>> firds.download_latest()
    >>> 
    >>> # Query reference data
    >>> instrument = firds.reference('US0378331005')
"""

__version__ = "0.2.0"
__author__ = "Robin"
__description__ = "ESMA Data Manager - Comprehensive wrapper for ESMA published data"

from .clients.firds import (
    FIRDSClient,
    FIRDSFile,
    FileType,
    AssetType,
    CommodityBaseProduct,
    OptionType,
    ExerciseStyle,
    DeliveryType,
    BondSeniority
)
from .clients.fitrs import FITRSClient
from .clients.benchmarks import BenchmarksClient
from .clients.ssr import SSRClient
from .config import Config
from .reference_api import ReferenceAPI

# Global reference API instance
reference = ReferenceAPI()

__all__ = [
    # Convenience functions
    "reference",
    
    # Clients
    "FIRDSClient",
    "FITRSClient", 
    "BenchmarksClient",
    "SSRClient",
    
    # FIRDS types
    "FIRDSFile",
    "FileType",
    "AssetType",
    "CommodityBaseProduct",
    "OptionType",
    "ExerciseStyle",
    "DeliveryType",
    "BondSeniority",
    
    # Config
    "Config",
]
