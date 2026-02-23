"""
FITRS File Manager - Comprehensive file management for FITRS data.

Handles listing, downloading, parsing, and caching operations for
FITRS (Financial Instruments Transparency System) data.
"""

import re
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
import pandas as pd

from esma_dm.file_manager.base import FileManager
from esma_dm.utils.constants import FITRS_SOLR_URL, FITRS_FILENAME_PATTERN
from esma_dm.utils import Utils
from ..enums import FITRSFileType as FileType, InstrumentType
from ..models import FITRSFile
from ..downloader import FileDownloader


class FITRSFileManager(FileManager):
    """
    Comprehensive file manager for FITRS (Financial Instruments Transparency System).
    
    Integrates all FITRS file operations:
    - List available files from ESMA with pagination
    - Download files with intelligent caching
    - Parse CSV files into DataFrames
    - Manage local cache
    - Extract file metadata
    - Support all file types (FULECR, DLTECR, FULNCR, DLTNCR, etc.)
    - Support both equity and non-equity instruments
    
    Example:
        >>> from esma_dm.file_manager.fitrs import FITRSFileManager
        >>> from esma_dm.config import Config
        >>> 
        >>> config = Config()
        >>> manager = FITRSFileManager(
        ...     cache_dir=config.downloads_path / 'fitrs',
        ...     date_from='2026-01-01',
        ...     date_to='2026-01-31'
        ... )
        >>> 
        >>> # List all equity FULECR files
        >>> files = manager.list_files(file_type='FULECR', fetch_all=True)
        >>> print(f"Found {len(files)} files")
        >>> 
        >>> # Download latest equity files
        >>> downloaded = manager.download_latest_full_files(instrument_type='equity')
        >>> 
        >>> # Get cache statistics
        >>> stats = manager.get_file_stats()
    """
    
    BASE_URL = FITRS_SOLR_URL
    FILENAME_PATTERN = re.compile(FITRS_FILENAME_PATTERN)
    
    def __init__(
        self,
        cache_dir: Path,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ):
        """
        Initialize FITRS file manager.
        
        Args:
            cache_dir: Directory for caching downloaded files
            date_from: Start date for file filtering (YYYY-MM-DD)
            date_to: End date for file filtering (YYYY-MM-DD)
        """
        # Set default dates
        date_from = date_from or '2018-01-01'
        date_to = date_to or datetime.today().strftime('%Y-%m-%d')
        
        # Initialize parent class
        super().__init__(
            base_url=self.BASE_URL,
            cache_dir=cache_dir,
            date_from=date_from,
            date_to=date_to,
            limit=1000
        )
        
        # Initialize downloader
        self.downloader = FileDownloader(cache_dir, logger=self.logger)
    
    def _build_filters(self, **kwargs) -> List[str]:
        """Build FITRS-specific filters for queries."""
        filters = []
        
        file_type = kwargs.get('file_type')
        instrument_type = kwargs.get('instrument_type')
        
        if file_type:
            filters.append(f'file_type:*{file_type}*')
        
        if instrument_type:
            if instrument_type.lower() == 'equity':
                filters.append('(file_type:*ECR*)')
            elif instrument_type.lower() == 'non-equity':
                filters.append('(file_type:*NCR*)')
        
        return filters
    
    def _filter_cached_files(self, files: List[Path], **kwargs) -> List[Path]:
        """Apply FITRS-specific filters to cached files."""
        file_type = kwargs.get('file_type')
        instrument_type = kwargs.get('instrument_type')
        
        filtered = files
        
        if file_type:
            filtered = [f for f in filtered if file_type in f.name]
        
        if instrument_type:
            if instrument_type.lower() == 'equity':
                filtered = [f for f in filtered if 'ECR' in f.name]
            elif instrument_type.lower() == 'non-equity':
                filtered = [f for f in filtered if 'NCR' in f.name]
        
        return filtered
    
    def _build_query_url(
        self,
        start: int = 0,
        rows: int = 1000,
        additional_filters: Optional[List[str]] = None
    ) -> str:
        """
        Build FITRS-specific SOLR query URL.
        
        FITRS uses 'creation_date' field instead of 'publication_date'.
        
        Args:
            start: Starting offset for results
            rows: Number of results to return
            additional_filters: Additional filter queries
            
        Returns:
            Complete query URL
        """
        query_url = (
            f"{self.base_url}?q=*"
            f"&fq=creation_date:[{self.date_from}T00:00:00Z+TO+{self.date_to}T23:59:59Z]"
        )
        
        # Add additional filters
        if additional_filters:
            for fq in additional_filters:
                query_url += f"&fq={fq}"
        
        query_url += f"&wt=xml&indent=true&start={start}&rows={rows}"
        
        return query_url
    
    def list_files(
        self,
        file_type: Optional[str] = None,
        instrument_type: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        fetch_all: bool = True
    ) -> List[FITRSFile]:
        """
        List available FITRS files from ESMA register.
        
        Args:
            file_type: Filter by file type (FULECR, DLTECR, FULNCR, DLTNCR, etc.)
            instrument_type: Filter by instrument type ('equity' or 'non-equity')
            date_from: Start date (YYYY-MM-DD), overrides instance date_from
            date_to: End date (YYYY-MM-DD), overrides instance date_to
            fetch_all: If True, fetch all pages using pagination
        
        Returns:
            List of FITRSFile objects
        
        Example:
            >>> files = manager.list_files(file_type='FULECR', fetch_all=True)
            >>> for f in files[:5]:
            ...     print(f"{f.filename} - {f.publication_date.strftime('%Y-%m-%d')}")
        """
        import requests
        
        # Use instance dates if not provided
        start_date = date_from or self.date_from
        end_date = date_to or self.date_to
        
        # Build query parts
        query_parts = []
        
        # Add file type filter
        if file_type:
            try:
                FileType(file_type)  # Validate
                query_parts.append(f'file_type:*{file_type}*')
            except ValueError:
                valid_types = [t.value for t in FileType]
                raise ValueError(f"Invalid file_type '{file_type}'. Must be one of: {valid_types}")
        
        # Add instrument type filter
        if instrument_type:
            if instrument_type.lower() == 'equity':
                query_parts.append('(file_type:*ECR*)')
            elif instrument_type.lower() == 'non-equity':
                query_parts.append('(file_type:*NCR*)')
            else:
                raise ValueError(f"Invalid instrument_type '{instrument_type}'. Must be 'equity' or 'non-equity'")
        
        # Build filter query string
        filter_query = ' AND '.join(query_parts) if query_parts else ''
        
        # Pagination loop
        all_files = []
        start = 0
        rows = 1000
        
        while True:
            # Build query URL
            query_url = (
                f"{self.BASE_URL}?q=*"
                f"&fq=creation_date:[{start_date}T00:00:00Z+TO+{end_date}T23:59:59Z]"
            )
            
            if filter_query:
                query_url += f"&fq={filter_query}"
            
            query_url += f"&wt=xml&indent=true&start={start}&rows={rows}"
            
            self.logger.debug(f"Fetching FITRS files: start={start}, rows={rows}")
            
            # Make request
            response = requests.get(query_url, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"Request failed with status {response.status_code}")
                self.logger.error(f"URL: {query_url}")
                raise Exception(f"Failed to fetch FITRS files: {response.status_code}")
            
            # Parse response
            df = self._utils.parse_xml_response(response)
            
            if df.empty:
                break
            
            # Convert rows to FITRSFile objects
            for _, row in df.iterrows():
                file_obj = self._doc_to_fitrs_file(row.to_dict())
                if file_obj:
                    all_files.append(file_obj)
            
            # Check if we got all results
            if len(df) < rows or not fetch_all:
                break
            
            start += rows
        
        self.logger.info(f"Found {len(all_files)} FITRS files")
        return all_files
    
    def download_latest_full_files(
        self,
        instrument_type: str = 'equity',
        update: bool = False
    ) -> List[Path]:
        """
        Download latest full FITRS files for a specific instrument type.
        
        Args:
            instrument_type: Instrument type ('equity' or 'non-equity')
            update: Force re-download even if cached
        
        Returns:
            List of paths to downloaded files
        
        Example:
            >>> paths = manager.download_latest_full_files(instrument_type='equity')
            >>> print(f"Downloaded {len(paths)} files")
        """
        # Determine file type based on instrument type
        if instrument_type.lower() == 'equity':
            file_type = 'FULECR'
        elif instrument_type.lower() == 'non-equity':
            file_type = 'FULNCR'
        else:
            raise ValueError(f"Invalid instrument_type '{instrument_type}'. Must be 'equity' or 'non-equity'")
        
        # List files
        files = self.list_files(file_type=file_type, fetch_all=True)
        
        if not files:
            self.logger.warning(f"No {file_type} files found")
            return []
        
        # Group by date and get latest
        files_by_date = {}
        for f in files:
            date_str = f.publication_date.strftime('%Y%m%d')
            if date_str not in files_by_date:
                files_by_date[date_str] = []
            files_by_date[date_str].append(f)
        
        latest_date = max(files_by_date.keys())
        latest_files = files_by_date[latest_date]
        
        self.logger.info(f"Found {len(latest_files)} files for latest date {latest_date}")
        
        # Download files
        downloaded = []
        for file_obj in latest_files:
            path = self.downloader.download_file(
                url=file_obj.download_link,
                filename=file_obj.filename,
                force=update,
                show_progress=True
            )
            if path:
                downloaded.append(path)
        
        return downloaded
    
    def parse_file(self, file_path: Path) -> pd.DataFrame:
        """
        Parse a FITRS CSV file into a DataFrame.
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            DataFrame containing file contents
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        self.logger.info(f"Parsing {file_path.name}")
        
        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
            return df
        except Exception as e:
            self.logger.error(f"Failed to parse {file_path.name}: {e}")
            raise
    
    def get_cached_files(
        self,
        file_type: Optional[str] = None,
        instrument_type: Optional[str] = None
    ) -> List[Path]:
        """
        List cached FITRS files with optional filtering.
        
        Args:
            file_type: Filter by file type
            instrument_type: Filter by instrument type ('equity' or 'non-equity')
        
        Returns:
            List of cached file paths
        """
        pattern = "*"
        
        if file_type:
            pattern = f"{file_type}*"
        elif instrument_type:
            if instrument_type.lower() == 'equity':
                pattern = "*ECR*"
            elif instrument_type.lower() == 'non-equity':
                pattern = "*NCR*"
        
        return self.downloader.get_cached_files(pattern)
    
    def get_file_stats(self) -> Dict[str, Any]:
        """
        Get statistics about cached FITRS files.
        
        Returns:
            Dictionary with file counts by type and instrument type
        """
        all_files = self.downloader.get_cached_files()
        
        stats = {
            'total_files': len(all_files),
            'total_size_mb': sum(f.stat().st_size for f in all_files) / (1024 * 1024),
            'by_file_type': {},
            'by_instrument_type': {
                'equity': 0,
                'non_equity': 0
            }
        }
        
        for f in all_files:
            # Count by file type
            for file_type in FileType:
                if file_type.value in f.name:
                    stats['by_file_type'][file_type.value] = stats['by_file_type'].get(file_type.value, 0) + 1
            
            # Count by instrument type
            if 'ECR' in f.name:
                stats['by_instrument_type']['equity'] += 1
            elif 'NCR' in f.name:
                stats['by_instrument_type']['non_equity'] += 1
        
        return stats
    
    def _parse_date(self, date_str: Optional[str]) -> datetime:
        """
        Parse date string to datetime object.
        
        Args:
            date_str: Date string in ISO format (YYYY-MM-DDTHH:MM:SSZ)
        
        Returns:
            Parsed datetime object
        """
        if not date_str:
            return datetime.now()
        
        try:
            # Handle ISO format with timezone
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            # Handle date-only format
            else:
                return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception:
            return datetime.now()
    
    def _doc_to_fitrs_file(self, doc: Dict[str, Any]) -> Optional[FITRSFile]:
        """
        Convert SOLR document to FITRSFile object.
        
        Args:
            doc: SOLR document dictionary
        
        Returns:
            FITRSFile object or None if parsing fails
        """
        try:
            filename = doc.get('file_name')
            if not filename:
                return None
            
            # Parse filename to extract metadata
            match = self.FILENAME_PATTERN.match(filename)
            if match:
                file_type = match.group(1)
                date_str = match.group(2)
                part_number = int(match.group(3))
                total_parts = int(match.group(4))
            else:
                # Fallback if pattern doesn't match
                file_type = "UNKNOWN"
                date_str = doc.get('creation_date', '').split('T')[0].replace('-', '')
                part_number = 1
                total_parts = 1
            
            # Determine instrument type
            instrument_type = 'equity' if 'ECR' in filename else 'non-equity' if 'NCR' in filename else None
            
            return FITRSFile(
                filename=filename,
                file_type=file_type,
                instrument_type=instrument_type,
                publication_date=self._parse_date(doc.get('creation_date')),
                download_link=doc.get('download_link'),
                file_size=doc.get('file_size'),
                part_number=part_number,
                total_parts=total_parts
            )
        except Exception as e:
            self.logger.warning(f"Failed to parse FITRS file metadata: {e}")
            return None
