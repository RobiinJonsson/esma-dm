"""
File Manager - Centralized file management for ESMA data.

This module provides a unified interface for managing ESMA data files
across different data sources (FIRDS, FITRS, DVCAP).

Note: Benchmarks data is accessed via API (see esma_dm.cli.benchmarks).
"""

from .base import FileManager
from .enums import (
    FIRDSFileType, FITRSFileType, DVCAPFileType, AssetType, InstrumentType,
    CommodityBaseProduct, OptionType, ExerciseStyle, DeliveryType, BondSeniority,
    FileType  # Backward compatibility alias
)
from .models import FileMetadata, FIRDSFile, FITRSFile, DVCAPFile
from .downloader import FileDownloader
from .firds import FIRDSFileManager
from .fitrs import FITRSFileManager
from .dvcap import DVCAPFileManager

__all__ = [
    # Base classes
    'FileManager',
    'FileDownloader',
    
    # FIRDS
    'FIRDSFileManager',
    'FIRDSFile',
    
    # FITRS
    'FITRSFileManager',
    'FITRSFile',
    
    # DVCAP
    'DVCAPFileManager',
    'DVCAPFile',
    
    # Enums
    'FIRDSFileType',
    'FITRSFileType',
    'DVCAPFileType',
    'AssetType',
    'InstrumentType',
    'CommodityBaseProduct',
    'OptionType',
    'ExerciseStyle',
    'DeliveryType',
    'BondSeniority',
    'FileType',  # Backward compatibility
    
    # Models
    'FileMetadata'
]
