"""
Base file manager class with common functionality for all ESMA data sources.
"""

import logging
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
import pandas as pd
import requests
from abc import ABC, abstractmethod

from esma_dm.utils import Utils


class FileManager(ABC):
    """
    Abstract base class for ESMA file management.
    
    Provides common functionality for:
    - Listing available files
    - Downloading files
    - Caching management
    - Date and type filtering
    - Pagination support
    """
    
    def __init__(
        self,
        base_url: str,
        cache_dir: Path,
        date_from: str,
        date_to: str,
        limit: int = 1000
    ):
        """
        Initialize file manager.
        
        Args:
            base_url: SOLR endpoint URL
            cache_dir: Directory for cached files
            date_from: Start date for file queries (YYYY-MM-DD)
            date_to: End date for file queries (YYYY-MM-DD)
            limit: Maximum results per request (default: 1000)
        """
        self.base_url = base_url
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.date_from = date_from
        self.date_to = date_to
        self.limit = min(limit, 1000)  # SOLR max per request
        self.logger = logging.getLogger(self.__class__.__name__)
        self._utils = Utils()
    
    def _build_query_url(
        self,
        start: int = 0,
        rows: int = 1000,
        additional_filters: Optional[List[str]] = None,
        sort: str = "publication_date desc"
    ) -> str:
        """
        Build SOLR query URL with filters.
        
        Args:
            start: Starting offset for results
            rows: Number of results to return
            additional_filters: Additional filter queries
            sort: Sort order (default: newest first)
            
        Returns:
            Complete query URL
        """
        query_url = (
            f"{self.base_url}?q=*"
            f"&fq=publication_date:[{self.date_from}T00:00:00Z+TO+{self.date_to}T23:59:59Z]"
        )
        
        # Add additional filters
        if additional_filters:
            for fq in additional_filters:
                query_url += f"&fq={fq}"
        
        # Add sort parameter
        if sort:
            query_url += f"&sort={sort}"
        
        query_url += f"&wt=xml&indent=true&start={start}&rows={rows}"
        
        return query_url
    
    def _fetch_page(
        self,
        start: int = 0,
        rows: int = 1000,
        additional_filters: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, int]:
        """
        Fetch a single page of results.
        
        Args:
            start: Starting offset
            rows: Number of results
            additional_filters: Additional filter queries
            
        Returns:
            Tuple of (DataFrame of results, total number of results available)
        """
        query_url = self._build_query_url(start, rows, additional_filters)
        
        self.logger.debug(f"Fetching page: start={start}, rows={rows}")
        response = requests.get(query_url)
        
        if response.status_code != 200:
            self.logger.error(f"Request failed with status {response.status_code}")
            raise Exception(f"Failed to fetch files: {response.status_code}")
        
        df = self._utils.parse_xml_response(response)
        
        # Extract total count from response (if available in numFound)
        total = len(df)  # Simple fallback, override in subclasses if SOLR provides numFound
        
        return df, total
    
    def list_files(
        self,
        file_type: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: Optional[int] = None,
        fetch_all: bool = False,
        **kwargs
    ) -> pd.DataFrame:
        """
        List available files with optional filters.
        
        Args:
            file_type: Filter by file type (data source specific)
            date_from: Override default start date
            date_to: Override default end date
            limit: Maximum number of results (None for unlimited)
            fetch_all: If True, fetch all results using pagination
            **kwargs: Additional data source specific filters
            
        Returns:
            DataFrame with file metadata
        """
        # Use override dates if provided
        query_date_from = date_from or self.date_from
        query_date_to = date_to or self.date_to
        
        # Build additional filters
        additional_filters = self._build_filters(file_type=file_type, **kwargs)
        
        # Determine fetch strategy
        if fetch_all or limit is None or limit > 1000:
            return self._fetch_all_pages(additional_filters, limit)
        else:
            # Single page fetch
            df, _ = self._fetch_page(start=0, rows=limit, additional_filters=additional_filters)
            return df
    
    def _fetch_all_pages(
        self,
        additional_filters: Optional[List[str]] = None,
        max_results: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch all pages of results using pagination.
        
        Args:
            additional_filters: Filter queries
            max_results: Maximum total results to fetch (None for unlimited)
            
        Returns:
            Combined DataFrame with all results
        """
        all_results = []
        start = 0
        rows = 1000  # Max per SOLR request
        total_fetched = 0
        
        while True:
            df, total = self._fetch_page(start, rows, additional_filters)
            
            if df.empty:
                break
            
            all_results.append(df)
            total_fetched += len(df)
            
            self.logger.info(f"Fetched {total_fetched} results...")
            
            # Check if we should continue
            if max_results and total_fetched >= max_results:
                break
            
            if len(df) < rows:
                # No more results available
                break
            
            start += rows
        
        if all_results:
            combined = pd.concat(all_results, ignore_index=True)
            if max_results:
                combined = combined.head(max_results)
            self.logger.info(f"Total files found: {len(combined)}")
            return combined
        else:
            return pd.DataFrame()
    
    @abstractmethod
    def _build_filters(self, **kwargs) -> List[str]:
        """
        Build data source specific filters.
        
        Must be implemented by subclasses.
        
        Returns:
            List of filter query strings
        """
        pass
    
    def list_cached_files(
        self,
        pattern: str = "*.csv",
        **kwargs
    ) -> List[Path]:
        """
        List files in cache directory.
        
        Args:
            pattern: File pattern to match (default: *.csv)
            **kwargs: Additional filters for subclass implementation
            
        Returns:
            List of file paths
        """
        files = list(self.cache_dir.glob(pattern))
        return self._filter_cached_files(files, **kwargs)
    
    @abstractmethod
    def _filter_cached_files(self, files: List[Path], **kwargs) -> List[Path]:
        """
        Apply data source specific filters to cached files.
        
        Must be implemented by subclasses.
        """
        pass
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about cached files.
        
        Returns:
            Dictionary with cache statistics
        """
        files = list(self.cache_dir.glob("*.csv"))
        
        if not files:
            return {
                'total_files': 0,
                'total_size_mb': 0,
                'cache_dir': str(self.cache_dir)
            }
        
        total_size = sum(f.stat().st_size for f in files)
        
        return {
            'total_files': len(files),
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'cache_dir': str(self.cache_dir),
            'oldest_file': min(f.stat().st_mtime for f in files),
            'newest_file': max(f.stat().st_mtime for f in files)
        }
