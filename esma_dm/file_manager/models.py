"""
Shared data models for ESMA file management.

Contains dataclasses used across FIRDS, FITRS, and other ESMA data sources.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class FileMetadata:
    """
    Generic file metadata for ESMA data files.
    
    Attributes:
        filename: Original filename
        file_type: Type of file (FULINS, DLTINS, FULECR, etc.)
        publication_date: Date the file was published
        download_link: URL to download the file
        file_size: Size of file in bytes (if available)
        part_number: Part number for multi-part files
        total_parts: Total number of parts for multi-part files
        asset_type: Asset type (C, D, E, F, H, I, J, O, R, S) - FIRDS only
        instrument_type: Instrument type (equity/non-equity) - FITRS only
    """
    filename: str
    file_type: str
    publication_date: datetime
    download_link: str
    file_size: Optional[int] = None
    part_number: Optional[int] = None
    total_parts: Optional[int] = None
    asset_type: Optional[str] = None
    instrument_type: Optional[str] = None
    
    def __str__(self) -> str:
        """String representation."""
        parts_info = ""
        if self.part_number and self.total_parts:
            parts_info = f" (Part {self.part_number}/{self.total_parts})"
        
        size_info = ""
        if self.file_size:
            size_mb = self.file_size / (1024 * 1024)
            size_info = f" - {size_mb:.2f} MB"
        
        return f"{self.filename}{parts_info}{size_info} - {self.publication_date.strftime('%Y-%m-%d')}"


@dataclass
class FIRDSFile(FileMetadata):
    """
    FIRDS-specific file metadata.
    
    Attributes:
        filename: Original filename
        file_type: Type of file (FULINS, DLTINS, FULCAN)
        asset_type: Asset type (C, D, E, F, H, I, J, O, R, S)
        publication_date: Date the file was published
        download_link: URL to download the file
        file_size: Size of file in bytes (if available)
        part_number: Part number for multi-part files
        total_parts: Total number of parts for multi-part files
    """
    pass


@dataclass
class FITRSFile(FileMetadata):
    """
    FITRS-specific file metadata.
    
    Attributes:
        filename: Original filename
        file_type: Type of file (FULECR, DLTECR, FULNCR, DLTNCR, etc.)
        instrument_type: Instrument type (equity/non-equity)
        publication_date: Date the file was published
        download_link: URL to download the file
        file_size: Size of file in bytes (if available)
        part_number: Part number for multi-part files
        total_parts: Total number of parts for multi-part files
    """
    pass


@dataclass
class DVCAPFile(FileMetadata):
    """
    DVCAP-specific file metadata.
    
    Attributes:
        filename: Original filename
        file_type: Type of file (DVCRES)
        publication_date: Date the file was published
        download_link: URL to download the file
        file_size: Size of file in bytes (if available)
    """
    pass


@dataclass
class BenchmarksFile(FileMetadata):
    """
    Benchmarks-specific file metadata.
    
    Attributes:
        filename: Original filename
        file_type: Type of file (BENCH_ARCHIVE)
        publication_date: Date the file was published
        download_link: URL to download the file
        file_size: Size of file in bytes (if available)
    """
    pass
