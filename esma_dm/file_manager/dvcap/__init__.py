"""
DVCAP (Double Volume Cap) file management module.

Provides file management for ESMA DVCAP data including:
- List available files from ESMA register
- Download and cache files
- Parse file metadata
- Manage local cache

Example:
    >>> from esma_dm.file_manager.dvcap import DVCAPFileManager
    >>> from esma_dm.config import Config
    >>> 
    >>> config = Config()
    >>> manager = DVCAPFileManager(cache_dir=config.downloads_path / 'dvcap')
    >>> files = manager.list_files()
"""

from ..enums import DVCAPFileType
from ..models import DVCAPFile
from .manager import DVCAPFileManager

__all__ = [
    'DVCAPFileManager',
    'DVCAPFileType',
    'DVCAPFile',
]
