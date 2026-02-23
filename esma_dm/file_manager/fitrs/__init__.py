"""
FITRS File Manager - Comprehensive file management for FITRS data.

This module handles all FITRS file operations:
- Listing available files
- Downloading files
- Parsing CSV files
- Managing cache
- Enumerations and models (imported from shared file_manager location)
"""

from ..enums import FITRSFileType as FileType, InstrumentType
from ..models import FITRSFile
from .manager import FITRSFileManager

__all__ = [
    'FITRSFileManager',
    'FileType',
    'InstrumentType',
    'FITRSFile'
]
