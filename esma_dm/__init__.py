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

__version__ = "0.3.0"
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
from .transparency_api import TransparencyAPI

# Global API instances (lazy-loaded to avoid import conflicts)
_reference_api = None
_transparency_api = None

def _get_reference_api():
    """Lazy-load reference API."""
    global _reference_api
    if _reference_api is None:
        _reference_api = ReferenceAPI()
    return _reference_api

def _get_transparency_api():
    """Lazy-load transparency API."""
    global _transparency_api
    if _transparency_api is None:
        _transparency_api = TransparencyAPI()
    return _transparency_api

# Create proxy objects for backwards compatibility
class _ReferenceProxy:
    def __getattr__(self, name):
        return getattr(_get_reference_api(), name)
    
    def __call__(self, *args, **kwargs):
        return _get_reference_api()(*args, **kwargs)

class _TransparencyProxy:
    def __getattr__(self, name):
        return getattr(_get_transparency_api(), name)
    
    def __call__(self, *args, **kwargs):
        return _get_transparency_api()(*args, **kwargs)

# Global API instances
reference = _ReferenceProxy()
transparency = _TransparencyProxy()

__all__ = [
    # Convenience functions
    "reference",
    "transparency",
    
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
