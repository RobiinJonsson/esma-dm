"""
FIRDS File Manager - Comprehensive file management for FIRDS data.

Integrates listing, downloading, parsing, and caching operations.
"""

import re
from typing import List, Optional, Dict, Any
from pathlib import Path
import pandas as pd

from esma_dm.file_manager.base import FileManager
from esma_dm.utils.constants import FIRDS_SOLR_URL, FIRDS_FILENAME_PATTERN
from esma_dm.utils import Utils
from ..enums import FIRDSFileType as FileType, AssetType
from ..models import FIRDSFile


class FIRDSFileManager(FileManager):
    """
    Comprehensive file manager for FIRDS (Financial Instruments Reference Data System).
    
    Integrates all FIRDS file operations:
    - List available files from ESMA with pagination
    - Download files with intelligent caching
    - Parse CSV files into DataFrames
    - Manage local cache
    - Extract file metadata
    - Support all file types (FULINS/DLTINS/FULCAN)
    - Support all asset types (C, D, E, F, H, I, J, O, R, S)
    
    Example:
        >>> from esma_dm.file_manager.firds import FIRDSFileManager
        >>> from esma_dm.config import Config
        >>> 
        >>> config = Config()
        >>> manager = FIRDSFileManager(
        ...     cache_dir=config.downloads_path / 'firds',
        ...     date_from='2026-01-01',
        ...     date_to='2026-01-31'
        ... )
        >>> 
        >>> # List all equity FULINS files
        >>> files = manager.list_files(file_type='FULINS', asset_type='E', fetch_all=True)
        >>> print(f"Found {len(files)} files")
        >>> 
        >>> # Download latest files
        >>> downloaded = manager.download_latest_full_files(asset_type='E')
        >>> 
        >>> # Parse a specific file
        >>> df = manager.parse_file(files.iloc[0]['download_link'])
    """
    
    def __init__(
        self,
        cache_dir: Path,
        date_from: str,
        date_to: str,
        limit: int = 1000,
        config=None
    ):
        """
        Initialize FIRDS file manager.
        
        Args:
            cache_dir: Directory for cached FIRDS files
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            limit: Maximum results per request (default: 1000)
            config: Optional config object for advanced operations
        """
        super().__init__(
            base_url=FIRDS_SOLR_URL,
            cache_dir=cache_dir,
            date_from=date_from,
            date_to=date_to,
            limit=limit
        )
        self.config = config
        self._utils = Utils()
    
    def _build_filters(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None,
        **kwargs
    ) -> List[str]:
        """
        Build FIRDS-specific filters.
        
        Args:
            file_type: FULINS, DLTINS, or FULCAN
            asset_type: C, D, E, F, H, I, J, O, R, S
            
        Returns:
            List of SOLR filter queries
        """
        filters = []
        
        # File name pattern filter using the file_name field directly
        if file_type or asset_type:
            pattern_parts = []
            
            if file_type:
                pattern_parts.append(file_type.upper())
            else:
                pattern_parts.append("(FULINS|DLTINS|FULCAN)")
            
            if asset_type:
                pattern_parts.append(f"_{asset_type.upper()}_")
            
            # Use file_name field for filtering
            if pattern_parts:
                # SOLR doesn't support regex in fq, so we'll filter post-fetch
                # But we can still optimize by searching for the file type
                if file_type:
                    filters.append(f"file_name:*{file_type.upper()}*")
        
        return filters
    
    def list_files(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: Optional[int] = None,
        fetch_all: bool = False
    ) -> pd.DataFrame:
        """
        List available FIRDS files with filtering.
        
        Args:
            file_type: Filter by FULINS, DLTINS, or FULCAN
            asset_type: Filter by asset type (C, D, E, F, H, I, J, O, R, S)
            date_from: Override default start date
            date_to: Override default end date
            limit: Maximum number of results (None for unlimited with fetch_all)
            fetch_all: If True, fetch all results using pagination
            
        Returns:
            DataFrame with file metadata:
            - file_name: Name of the file
            - file_type: Extracted type (FULINS/DLTINS/FULCAN)
            - asset_type: Extracted asset type
            - publication_date: Publication date
            - download_link: Download URL
            - file_size: Size in bytes
            
        Example:
            >>> # Get all equity FULINS files (automatic pagination)
            >>> files = manager.list_files(file_type='FULINS', asset_type='E', fetch_all=True)
            >>> print(f"Found {len(files)} files")
            >>> 
            >>> # Get latest 100 debt files
            >>> recent = manager.list_files(asset_type='D', limit=100)
        """
        # Override dates if provided
        original_date_from = self.date_from
        original_date_to = self.date_to
        
        if date_from:
            self.date_from = date_from
        if date_to:
            self.date_to = date_to
        
        try:
            # Fetch files
            df = super().list_files(
                file_type=file_type,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                fetch_all=fetch_all,
                asset_type=asset_type
            )
            
            if df.empty:
                return df
            
            # Post-fetch filtering (since SOLR doesn't support regex)
            if file_type:
                file_type_upper = file_type.upper()
                df = df[df['file_name'].str.contains(file_type_upper, na=False)]
            
            if asset_type:
                asset_type_upper = asset_type.upper()
                pattern = rf"_{asset_type_upper}_"
                df = df[df['file_name'].str.contains(pattern, na=False, regex=True)]
            
            # Extract metadata from filenames
            df = self._extract_file_metadata(df)
            
            return df
            
        finally:
            # Restore original dates
            self.date_from = original_date_from
            self.date_to = original_date_to
    
    def _extract_file_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract metadata from FIRDS filenames.
        
        Filename pattern: {TYPE}_{ASSET}_{DATE}_{PART}of{TOTAL}_data.csv
        Example: FULINS_E_20260117_01of02_data.csv
        
        Args:
            df: DataFrame with file_name column
            
        Returns:
            DataFrame with additional columns:
            - file_type: FULINS, DLTINS, or FULCAN
            - asset_type: Single character asset type
            - file_date: Date from filename
            - part_number: Part number
            - total_parts: Total parts
        """
        if 'file_name' not in df.columns or df.empty:
            return df
        
        df = df.copy()
        
        # Extract using regex
        pattern = re.compile(FIRDS_FILENAME_PATTERN)
        
        # Initialize new columns
        df['file_type'] = None
        df['asset_type'] = None
        df['file_date'] = None
        df['part_number'] = None
        df['total_parts'] = None
        
        # Parse each filename
        for idx, row in df.iterrows():
            filename = str(row['file_name'])
            parsed = self._parse_filename(filename, pattern)
            
            df.at[idx, 'file_type'] = parsed['file_type']
            df.at[idx, 'asset_type'] = parsed['asset_type']
            df.at[idx, 'file_date'] = parsed['file_date']
            df.at[idx, 'part_number'] = parsed['part_number']
            df.at[idx, 'total_parts'] = parsed['total_parts']
        
        return df
    
    def _parse_filename(self, filename: str, pattern: re.Pattern) -> Dict[str, Any]:
        """Parse a single FIRDS filename."""
        match = pattern.search(filename)
        
        if match:
            return {
                'file_type': match.group(1),
                'asset_type': match.group(2),
                'file_date': match.group(3),
                'part_number': int(match.group(4)),
                'total_parts': int(match.group(5))
            }
        else:
            return {
                'file_type': None,
                'asset_type': None,
                'file_date': None,
                'part_number': None,
                'total_parts': None
            }
    
    def _filter_cached_files(
        self,
        files: List[Path],
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> List[Path]:
        """
        Filter cached files by FIRDS-specific criteria.
        
        Args:
            files: List of file paths
            file_type: Filter by FULINS, DLTINS, or FULCAN
            asset_type: Filter by asset type
            
        Returns:
            Filtered list of file paths
        """
        filtered = files
        
        if file_type:
            file_type_upper = file_type.upper()
            filtered = [f for f in filtered if file_type_upper in f.name.upper()]
        
        if asset_type:
            asset_type_upper = asset_type.upper()
            pattern = rf"_{asset_type_upper}_"
            filtered = [f for f in filtered if re.search(pattern, f.name.upper())]
        
        return filtered
    
    def list_cached_files(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> List[Path]:
        """
        List FIRDS files in cache directory.
        
        Args:
            file_type: Filter by FULINS, DLTINS, or FULCAN
            asset_type: Filter by asset type (C, D, E, F, H, I, J, O, R, S)
            
        Returns:
            List of matching file paths
            
        Example:
            >>> cached = manager.list_cached_files(file_type='FULINS', asset_type='E')
            >>> print(f"Found {len(cached)} cached equity FULINS files")
        """
        return super().list_cached_files(
            pattern="*.csv",
            file_type=file_type,
            asset_type=asset_type
        )
    
    def get_file_stats(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get statistics for FIRDS files.
        
        Args:
            file_type: Filter by file type
            asset_type: Filter by asset type
            
        Returns:
            Dictionary with file statistics:
            - total_files: Number of files
            - by_type: Breakdown by file type
            - by_asset: Breakdown by asset type
            - total_size_mb: Total size in MB
        """
        cached = self.list_cached_files(file_type=file_type, asset_type=asset_type)
        
        if not cached:
            return {
                'total_files': 0,
                'by_type': {},
                'by_asset': {},
                'total_size_mb': 0
            }
        
        # Count by type and asset
        by_type = {}
        by_asset = {}
        total_size = 0
        
        pattern = re.compile(FIRDS_FILENAME_PATTERN)
        
        for file_path in cached:
            match = pattern.search(file_path.name)
            if match:
                ftype = match.group(1)
                atype = match.group(2)
                
                by_type[ftype] = by_type.get(ftype, 0) + 1
                by_asset[atype] = by_asset.get(atype, 0) + 1
            
            total_size += file_path.stat().st_size
        
        return {
            'total_files': len(cached),
            'by_type': by_type,
            'by_asset': by_asset,
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }
    
    def download_file(self, url: str, update: bool = False) -> pd.DataFrame:
        """
        Download and parse a specific FIRDS file by URL.
        
        Args:
            url: Direct download URL from ESMA
            update: Force re-download even if cached
            
        Returns:
            Parsed DataFrame with instrument data
            
        Example:
            >>> df = manager.download_file('https://registers.esma.europa.eu/...')
            >>> print(f"Downloaded {len(df)} instruments")
        """
        return self._utils.download_and_parse_file(url, data_type='firds', update=update)
    
    def download_latest_full_files(
        self,
        asset_type: str,
        update: bool = False,
        cleanup_old: bool = True
    ) -> List[Path]:
        """
        Download latest FULINS files for a specific asset type.
        
        Args:
            asset_type: Asset type code (C, D, E, F, H, I, J, O, R, S)
            update: Force fresh download even if cached
            cleanup_old: Remove older files of same type/asset after download (default: True)
            
        Returns:
            List of paths to downloaded files
            
        Example:
            >>> files = manager.download_latest_full_files(asset_type='E')
            >>> print(f"Downloaded {len(files)} files")
        """
        # Get list of latest files (SOLR now sorts by publication_date DESC)
        files_df = self.list_files(
            file_type='FULINS',
            asset_type=asset_type,
            fetch_all=False,
            limit=50  # Reduced since we're getting newest first
        )
        
        if files_df.empty:
            self.logger.warning(f"No FULINS files found for asset type {asset_type}")
            return []
        
        # Extract date and get latest
        if 'file_date' in files_df.columns:
            max_date = files_df['file_date'].max()
            latest_files = files_df[files_df['file_date'] == max_date]
        else:
            # If no file_date, take first row (newest due to sort)
            latest_files = files_df.head(1)
        
        self.logger.info(f"Found {len(latest_files)} files for asset type {asset_type} (latest date)")
        
        # Clean up old files BEFORE download if enabled
        if cleanup_old:
            self._cleanup_old_files(file_type='FULINS', asset_type=asset_type, keep_latest=False)
        
        # Download each file
        downloaded_files = []
        for _, row in latest_files.iterrows():
            url = row['download_link']
            self.logger.info(f"Downloading {row['file_name']}...")
            
            try:
                df = self.download_file(url, update=update)
                if not df.empty:
                    # File is cached after download, get the cached path
                    file_name = row['file_name'].replace('.zip', '_data.csv')
                    cached_path = self.cache_dir / file_name
                    if cached_path.exists():
                        downloaded_files.append(cached_path)
            except Exception as e:
                self.logger.error(f"Failed to download {row['file_name']}: {e}")
        
        return downloaded_files
    
    def _cleanup_old_files(
        self,
        file_type: str,
        asset_type: str,
        keep_latest: bool = True
    ) -> int:
        """
        Remove old cached files of specified type and asset.
        
        Args:
            file_type: FULINS, DLTINS, or FULCAN
            asset_type: Asset type code
            keep_latest: If True, keep the most recent file
            
        Returns:
            Number of files deleted
        """
        # Build pattern for matching files
        pattern = f"{file_type}_{asset_type}_*_data.csv"
        matching_files = sorted(
            self.cache_dir.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True  # Newest first
        )
        
        if not matching_files:
            return 0
        
        # Determine which files to delete
        if keep_latest and len(matching_files) > 1:
            files_to_delete = matching_files[1:]  # Keep first (newest), delete rest
        elif not keep_latest:
            files_to_delete = matching_files  # Delete all
        else:
            files_to_delete = []
        
        # Delete files
        deleted_count = 0
        for file_path in files_to_delete:
            try:
                file_path.unlink()
                self.logger.info(f"Deleted old file: {file_path.name}")
                deleted_count += 1
            except Exception as e:
                self.logger.warning(f"Failed to delete {file_path.name}: {e}")
        
        if deleted_count > 0:
            self.logger.info(f"Cleaned up {deleted_count} old file(s)")
        
        return deleted_count
    
    def get_latest_metadata(
        self,
        asset_type: str,
        file_type: str = 'FULINS'
    ) -> List[FIRDSFile]:
        """
        Get metadata for latest files of a specific type.
        
        Args:
            asset_type: Asset type code
            file_type: FULINS or DLTINS
            
        Returns:
            List of FIRDSFile objects with metadata
            
        Example:
            >>> metadata = manager.get_latest_metadata(asset_type='E')
            >>> for file in metadata:
            ...     print(f"{file.file_name} - Part {file.part_number}/{file.total_parts}")
        """
        df = self.list_files(
            file_type=file_type,
            asset_type=asset_type,
            fetch_all=False,
            limit=100
        )
        
        if df.empty:
            return []
        
        # Get latest date
        if 'file_date' in df.columns:
            max_date = df['file_date'].max()
            df = df[df['file_date'] == max_date]
        
        return [FIRDSFile.from_row(row) for _, row in df.iterrows()]
    
    def parse_file(self, file_path: Path) -> pd.DataFrame:
        """
        Parse a local FIRDS CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Parsed DataFrame
        """
        return pd.read_csv(file_path)
    
    def get_available_asset_types(self, file_type: Optional[str] = None) -> List[str]:
        """
        Get list of asset types that have available files.
        
        Args:
            file_type: Optional filter by file type
            
        Returns:
            List of asset type codes
        """
        df = self.list_files(file_type=file_type, fetch_all=True)
        
        if df.empty or 'asset_type' not in df.columns:
            return []
        
        asset_types = df['asset_type'].dropna().unique().tolist()
        return sorted(asset_types)
