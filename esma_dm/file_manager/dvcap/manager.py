"""
DVCAP File Manager - Comprehensive file management for DVCAP data.

Handles listing, downloading, parsing, and caching operations for
DVCAP (Double Volume Cap) data.
"""

import re
from typing import List, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
import pandas as pd

from esma_dm.file_manager.base import FileManager
from esma_dm.utils.constants import DVCAP_SOLR_URL
from esma_dm.utils import Utils
from ..enums import DVCAPFileType as FileType
from ..models import DVCAPFile
from ..downloader import FileDownloader


class DVCAPFileManager(FileManager):
    """
    Comprehensive file manager for DVCAP (Double Volume Cap).
    
    Integrates all DVCAP file operations:
    - List available files from ESMA with pagination
    - Download files with intelligent caching
    - Parse CSV files into DataFrames
    - Manage local cache
    - Extract file metadata
    
    Example:
        >>> from esma_dm.file_manager.dvcap import DVCAPFileManager
        >>> from esma_dm.config import Config
        >>> 
        >>> config = Config()
        >>> manager = DVCAPFileManager(
        ...     cache_dir=config.downloads_path / 'dvcap',
        ...     date_from='2022-01-01',
        ...     date_to='2026-01-31'
        ... )
        >>> 
        >>> # List all DVCAP files
        >>> files = manager.list_files(fetch_all=True)
        >>> print(f"Found {len(files)} files")
        >>> 
        >>> # Download latest file
        >>> downloaded = manager.download_latest_file()
        >>> 
        >>> # Get cache statistics
        >>> stats = manager.get_file_stats()
    """
    
    BASE_URL = DVCAP_SOLR_URL
    FILENAME_PATTERN = re.compile(r'DVCRES_(\d{8})\.zip')
    
    def __init__(
        self,
        cache_dir: Path,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ):
        """
        Initialize DVCAP file manager.
        
        Args:
            cache_dir: Directory for caching downloaded files
            date_from: Start date for file filtering (YYYY-MM-DD)
            date_to: End date for file filtering (YYYY-MM-DD)
        """
        # Set default dates
        date_from = date_from or '2022-01-01'  # DVCAP data starts from 2022
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
        """Build DVCAP-specific filters for queries (none needed)."""
        return []
    
    def _filter_cached_files(self, files: List[Path], **kwargs) -> List[Path]:
        """Apply DVCAP-specific filters to cached files."""
        return files
    
    def _build_query_url(
        self,
        start: int = 0,
        rows: int = 1000,
        additional_filters: Optional[List[str]] = None
    ) -> str:
        """
        Build DVCAP-specific SOLR query URL.
        
        DVCAP uses 'creation_date' field like FITRS.
        
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
    
    def _doc_to_dvcap_file(self, doc: Dict[str, Any]) -> DVCAPFile:
        """
        Convert SOLR document to DVCAPFile object.
        
        Args:
            doc: Raw document from SOLR API
            
        Returns:
            DVCAPFile object with parsed metadata
        """
        filename = doc.get('file_name', '')
        download_link = doc.get('download_link', '')
        
        # Parse creation date
        creation_date_str = doc.get('creation_date', '')
        try:
            publication_date = datetime.fromisoformat(creation_date_str.replace('Z', '+00:00'))
        except ValueError:
            publication_date = datetime.now()
        
        # Extract date from filename (DVCRES_YYYYMMDD.zip)
        match = self.FILENAME_PATTERN.match(filename)
        date_extracted = None
        if match:
            date_extracted = match.group(1)
        
        return DVCAPFile(
            filename=filename,
            file_type='DVCRES',
            publication_date=publication_date,
            download_link=download_link,
            file_size=doc.get('file_size'),
        )
    
    def list_files(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        fetch_all: bool = True
    ) -> List[DVCAPFile]:
        """
        List available DVCAP files from ESMA register.
        
        Args:
            date_from: Start date (YYYY-MM-DD), overrides instance date_from
            date_to: End date (YYYY-MM-DD), overrides instance date_to
            fetch_all: If True, fetch all pages using pagination
        
        Returns:
            List of DVCAPFile objects
        
        Example:
            >>> files = manager.list_files(fetch_all=True)
            >>> for f in files[:5]:
            ...     print(f"{f.filename} - {f.publication_date.strftime('%Y-%m-%d')}")
        """
        import requests
        
        # Use instance dates if not provided
        start_date = date_from or self.date_from
        end_date = date_to or self.date_to
        
        # Pagination loop
        all_files = []
        start = 0
        rows = 1000
        
        while True:
            # Build query URL
            query_url = (
                f"{self.BASE_URL}?q=*"
                f"&fq=creation_date:[{start_date}T00:00:00Z+TO+{end_date}T23:59:59Z]"
                f"&wt=xml&indent=true&start={start}&rows={rows}"
            )
            
            self.logger.debug(f"Fetching DVCAP files: start={start}, rows={rows}")
            
            # Make request
            response = requests.get(query_url, timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"Request failed with status {response.status_code}")
                raise Exception(f"Failed to fetch DVCAP files: {response.status_code}")
            
            # Parse response
            df = self._utils.parse_xml_response(response)
            
            if df.empty:
                break
            
            # Convert rows to DVCAPFile objects
            for _, row in df.iterrows():
                file_obj = self._doc_to_dvcap_file(row.to_dict())
                all_files.append(file_obj)
            
            # Check if we should continue pagination
            if not fetch_all or len(df) < rows:
                break
            
            start += rows
        
        self.logger.info(f"Found {len(all_files)} DVCAP files")
        return all_files
    
    def download_latest_file(self, update: bool = False) -> Optional[Path]:
        """
        Download the most recent DVCAP file.
        
        Args:
            update: Force fresh download even if cached
        
        Returns:
            Path to downloaded CSV file (extracted from ZIP), or None if no files found
        
        Example:
            >>> file_path = manager.download_latest_file()
            >>> if file_path:
            ...     df = pd.read_csv(file_path)
        """
        # Get all files
        files = self.list_files(fetch_all=True)
        
        if not files:
            self.logger.warning("No DVCAP files found")
            return None
        
        # Sort by publication date, get most recent
        latest = max(files, key=lambda f: f.publication_date)
        
        self.logger.info(f"Latest file: {latest.filename} ({latest.publication_date.strftime('%Y-%m-%d')})")
        
        # Download and extract
        zip_path = self.downloader.download_file(
            url=latest.download_link,
            filename=latest.filename,
            force=update
        )
        
        if not zip_path:
            return None
        
        # Extract ZIP
        extract_dir = self.downloader.extract_zip(zip_path)
        
        if not extract_dir:
            return None
        
        # Find CSV file in extracted directory
        csv_files = list(extract_dir.glob("*.csv"))
        if csv_files:
            return csv_files[0]
        
        return None
    
    def parse_file(self, file_path: Path) -> pd.DataFrame:
        """
        Parse a DVCAP CSV file into a DataFrame.
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            DataFrame with parsed data
        
        Example:
            >>> df = manager.parse_file(Path('DVCRES_20260101.csv'))
            >>> print(df.head())
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        self.logger.info(f"Parsing DVCAP file: {file_path.name}")
        
        try:
            df = pd.read_csv(file_path, sep=',', encoding='utf-8')
            self.logger.info(f"Parsed {len(df)} records from {file_path.name}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to parse {file_path.name}: {e}")
            raise
    
    def list_cached_files(self) -> List[Path]:
        """
        List DVCAP files in cache directory.
        
        Returns:
            List of cached CSV file paths
        """
        return super().list_cached_files(pattern="*.csv")
    
    def get_file_stats(self) -> Dict[str, Any]:
        """
        Get statistics for DVCAP files.
        
        Returns:
            Dictionary with file statistics:
            - total_files: Number of files
            - total_size_mb: Total size in MB
            - files: List of file details
        """
        cached = self.list_cached_files()
        
        if not cached:
            return {
                'total_files': 0,
                'total_size_mb': 0,
                'files': []
            }
        
        total_size = sum(f.stat().st_size for f in cached)
        
        files_info = []
        for file_path in cached:
            stat = file_path.stat()
            files_info.append({
                'name': file_path.name,
                'size_mb': stat.st_size / (1024 * 1024),
                'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return {
            'total_files': len(cached),
            'total_size_mb': total_size / (1024 * 1024),
            'files': files_info
        }
