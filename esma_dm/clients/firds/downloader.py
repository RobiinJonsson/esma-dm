"""
FIRDS file download and management operations.
"""

import logging
from typing import List, Optional, Dict, Any
import pandas as pd
import requests

from .enums import FileType, AssetType
from .models import FIRDSFile
from esma_dm.utils import Utils
from esma_dm.utils.validators import validate_isin
from esma_dm.utils.constants import FIRDS_SOLR_URL


class FIRDSDownloader:
    """Handles downloading and file management for FIRDS data."""
    
    def __init__(self, config, firds_config, date_from: str, date_to: str, limit: int):
        """Initialize downloader with configuration."""
        self.config = config
        self.firds_config = firds_config
        self.date_from = date_from
        self.date_to = date_to
        self.limit = limit
        self.BASE_URL = FIRDS_SOLR_URL
        self.logger = logging.getLogger(__name__)
        self._utils = Utils()
    
    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """Find column in DataFrame matching any of the patterns."""
        for pattern in patterns:
            matches = [col for col in df.columns if pattern in col]
            if matches:
                return matches[0]
        return None
    
    def get_file_list(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve list of available FIRDS files.
        
        Args:
            file_type: Filter by file type ("FULINS" or "DLTINS")
            asset_type: Filter by asset type (C, D, E, F, H, I, J, O, R, S)
        
        Returns:
            DataFrame containing file metadata including:
            - file_name: Name of the file
            - file_type: Type (Full or Delta)
            - publication_date: When file was published
            - download_link: URL to download file
        
        Example:
            >>> # Downloader is typically initialized by FIRDSClient
            >>> # with appropriate configuration
            >>> files = downloader.get_file_list()
            >>> equity_fulins = downloader.get_file_list(file_type='FULINS', asset_type='E')
        """
        query_url = (
            f"{self.BASE_URL}?q=*"
            f"&fq=publication_date:[{self.date_from}T00:00:00Z+TO+{self.date_to}T23:59:59Z]"
            f"&wt=xml&indent=true&start=0&rows={self.limit}"
        )
        
        self.logger.info(f"Requesting FIRDS file list")
        response = requests.get(query_url)
        
        if response.status_code != 200:
            self.logger.error(f"Request failed with status code {response.status_code}")
            raise Exception(f"Failed to fetch FIRDS files: {response.status_code}")
        
        self.logger.info("Request successful, parsing response")
        df = self._utils.parse_xml_response(response)
        
        # Apply filters
        if file_type:
            file_type_upper = file_type.upper()
            df = df[df['file_name'].str.contains(file_type_upper, na=False)]
        
        if asset_type:
            asset_type_upper = asset_type.upper()
            pattern = rf"_{asset_type_upper}_"
            df = df[df['file_name'].str.contains(pattern, na=False)]
        
        return df
    
    def get_files_metadata(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> List[FIRDSFile]:
        """
        Get structured metadata for available FIRDS files.
        
        Args:
            file_type: Filter by file type ("FULINS" or "DLTINS")
            asset_type: Filter by asset type (C, D, E, F, H, I, J, O, R, S)
        
        Returns:
            List of FIRDSFile objects with parsed metadata
        
        Example:
            >>> files = downloader.get_files_metadata(file_type='FULINS', asset_type='E')
            >>> for f in files[:5]:
            ...     print(f"{f.file_name} - {f.publication_date} - Part {f.part_number}/{f.total_parts}")
        """
        df = self.get_file_list(file_type=file_type, asset_type=asset_type)
        return [FIRDSFile.from_row(row) for _, row in df.iterrows()]
    
    def get_latest_full_files(
        self,
        asset_type: str = "E",
        isin_filter: Optional[List[str]] = None,
        update: bool = False,
        auto_cleanup: bool = False,
        keep_files: int = 2
    ) -> pd.DataFrame:
        """
        Retrieve the latest full FIRDS files for a specific asset type.
        Intelligently chooses between API results and cached files.
        
        Args:
            asset_type: CFI first character (C, D, E, F, H, I, J, O, R, S)
            isin_filter: Optional list of ISINs to filter results
            update: Force re-download of files (default: False, use cached data)
            auto_cleanup: Automatically remove old cached files after download
            keep_files: Number of newest files to keep during cleanup (default: 2)
        
        Returns:
            DataFrame containing instrument reference data
        
        Example:
            >>> equities = downloader.get_latest_full_files(asset_type='E')
            >>> # Download with automatic cleanup
            >>> equities = downloader.get_latest_full_files(
            ...     asset_type='E', 
            ...     update=True, 
            ...     auto_cleanup=True
            ... )
        """
        # Validate asset type
        try:
            AssetType(asset_type)
        except ValueError:
            valid_types = [t.value for t in AssetType]
            raise ValueError(f"Invalid asset_type '{asset_type}'. Must be one of: {valid_types}")
        
        # Check for newer cached files first (unless update=True)
        if not update and self.firds_config.cache_enabled:
            cached_data = self._check_cached_files(asset_type)
            if cached_data is not None:
                # Apply ISIN filter if provided
                if isin_filter and 'Id' in cached_data.columns:
                    cached_data = cached_data[cached_data['Id'].isin(isin_filter)]
                return cached_data
        
        # Get file list with filters
        files = self.get_file_list(file_type='FULINS', asset_type=asset_type)
        
        # Filter by file_type column if it exists and has values
        if not files.empty and 'file_type' in files.columns:
            full_files = files[files['file_type'].str.contains('Full', case=False, na=False)]
            # If file_type filtering removes all results, keep original (filename filtering is enough)
            if not full_files.empty:
                files = full_files
        
        if files.empty:
            self.logger.warning(f"No FULINS files found for asset type {asset_type}")
            return pd.DataFrame()
        
        # Extract date and get latest
        files['date'] = files['file_name'].str.extract(r'_(\d{8})_')[0]
        max_date = files['date'].max()
        latest_files = files[files['date'] == max_date]
        
        self.logger.info(f"Found {len(latest_files)} files for latest date {max_date}")
        
        # Download and parse files
        dfs = []
        for url in latest_files['download_link'].unique():
            self.logger.info(f"Downloading and parsing {url}")
            df = self._utils.download_and_parse_file(url, data_type='firds', update=update)
            
            # Apply ISIN filter if provided
            if isin_filter and 'Id' in df.columns:
                df = df[df['Id'].isin(isin_filter)]
            
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        result = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Retrieved {len(result)} instruments")
        
        # Perform cleanup if requested
        if auto_cleanup:
            cleanup_result = self.cleanup_old_files(
                asset_type=asset_type, 
                keep_count=keep_files,
                file_type='FULINS'
            )
            self.logger.info(f"Cleanup completed: {cleanup_result['message']}")
        
        return result
    
    def _check_cached_files(self, asset_type: str) -> Optional[pd.DataFrame]:
        """
        Check for cached files that are newer than API results.
        
        Args:
            asset_type: Asset type to check
            
        Returns:
            Combined DataFrame from cached files if newer than API, else None
        """
        import os
        import pandas as pd
        from datetime import datetime
        
        cache_dir = self.config.downloads_path / 'firds'
        if not cache_dir.exists():
            return None
        
        # Find cached FULINS files for this asset type
        pattern = f"FULINS_{asset_type}_"
        cached_files = [
            f for f in os.listdir(cache_dir)
            if f.startswith(pattern) and f.endswith('_data.csv')
        ]
        
        if not cached_files:
            return None
        
        # Extract dates from cached files
        cached_dates = []
        for filename in cached_files:
            try:
                # Extract date: FULINS_S_20260103_01of05_data.csv -> 20260103
                date_str = filename.split('_')[2]
                cached_dates.append(date_str)
            except (IndexError, ValueError):
                continue
        
        if not cached_dates:
            return None
        
        # Get the newest cached date
        max_cached_date = max(cached_dates)
        
        # Compare with API latest (if available)
        try:
            files = self.get_file_list(file_type='FULINS', asset_type=asset_type)
            if not files.empty:
                files['date'] = files['file_name'].str.extract(r'_(\d{8})_')[0]
                api_max_date = files['date'].max()
                
                # Use cached if newer or equal to API
                if max_cached_date >= api_max_date:
                    self.logger.info(f"Using cached files from {max_cached_date} (API has {api_max_date})")
                    return self._load_cached_files(asset_type, max_cached_date)
                else:
                    self.logger.info(f"API has newer files ({api_max_date}) than cache ({max_cached_date})")
                    return None
            else:
                # No API results, use cached
                self.logger.info(f"API returned no results, using cached files from {max_cached_date}")
                return self._load_cached_files(asset_type, max_cached_date)
                
        except Exception as e:
            self.logger.warning(f"Error checking API: {e}, using cached files")
            return self._load_cached_files(asset_type, max_cached_date)
    
    def _load_cached_files(self, asset_type: str, date: str) -> pd.DataFrame:
        """Load and combine all cached files for a specific asset type and date."""
        import os
        import pandas as pd
        
        cache_dir = self.config.downloads_path / 'firds'
        pattern = f"FULINS_{asset_type}_{date}_"
        
        cached_files = [
            os.path.join(cache_dir, f)
            for f in os.listdir(cache_dir)
            if f.startswith(pattern) and f.endswith('_data.csv')
        ]
        
        if not cached_files:
            return pd.DataFrame()
        
        dfs = []
        for file_path in cached_files:
            try:
                df = pd.read_csv(file_path)
                if not df.empty:
                    dfs.append(df)
                    self.logger.info(f"Loaded {len(df)} records from {os.path.basename(file_path)}")
            except Exception as e:
                self.logger.warning(f"Failed to load {file_path}: {e}")
        
        if dfs:
            result = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Combined {len(result)} total records from cached files")
            return result
        
        return pd.DataFrame()
    
    def get_instruments(
        self,
        isin_list: List[str],
        asset_type: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Retrieve reference data for specific ISINs.
        
        Args:
            isin_list: List of ISINs to retrieve
            asset_type: Optional asset type filter for efficiency
        
        Returns:
            DataFrame with reference data for requested ISINs
        
        Example:
            >>> instruments = downloader.get_instruments([
            ...     'GB00B1YW4409',  # Sage Group
            ...     'US0378331005',  # Apple Inc
            ... ])
        """
        if asset_type:
            return self.get_latest_full_files(asset_type=asset_type, isin_filter=isin_list)
        
        # Try all asset types if not specified
        all_results = []
        for at in AssetType:
            try:
                df = self.get_latest_full_files(asset_type=at.value, isin_filter=isin_list)
                if not df.empty:
                    all_results.append(df)
            except Exception as e:
                self.logger.warning(f"Error fetching {at.value}: {e}")
                continue
        
        if not all_results:
            self.logger.warning(f"No data found for ISINs: {isin_list}")
            return pd.DataFrame()
        
        return pd.concat(all_results, ignore_index=True)
    
    def get_delta_files(
        self,
        asset_type: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        update: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve delta (incremental) FIRDS files for a specific asset type.
        
        DLTINS files contain daily changes (new instruments, modifications, terminations).
        
        Args:
            asset_type: CFI first character (C, D, E, F, H, I, J, O, R, S)
            date_from: Start date (defaults to instance date_from)
            date_to: End date (defaults to instance date_to)
            update: Force re-download of files (default: False, use cached data)
        
        Returns:
            DataFrame containing instrument changes
        
        Example:
            >>> changes = downloader.get_delta_files(
            ...     asset_type='E',
            ...     date_from='2024-01-01',
            ...     date_to='2024-01-31'
            ... )
        """
        # Temporarily override date range if specified
        original_from = self.date_from
        original_to = self.date_to
        
        if date_from:
            self.date_from = date_from
        if date_to:
            self.date_to = date_to
        
        try:
            # Validate asset type
            try:
                AssetType(asset_type)
            except ValueError:
                valid_types = [t.value for t in AssetType]
                raise ValueError(f"Invalid asset_type '{asset_type}'. Must be one of: {valid_types}")
            
            # Get file list WITHOUT asset_type filter (DLTINS files don't have asset type in filename)
            # DLTINS files are like: DLTINS_20260101_01of01.zip (contain all asset types)
            files = self.get_file_list(file_type='DLTINS', asset_type=None)
            
            if files.empty:
                self.logger.warning(f"No DLTINS files found")
                return pd.DataFrame()
            
            self.logger.info(f"Found {len(files)} delta files")
            
            # Download and parse files, then filter by asset type
            dfs = []
            for url in files['download_link'].unique():
                self.logger.info(f"Downloading and parsing {url}")
                df = self._utils.download_and_parse_file(url, data_type='firds', update=update)
                
                if not df.empty:
                    # Filter by asset type after parsing (check CFI code first character)
                    cfi_col = self._find_column(df, ['ClssfctnTp', 'CfiCd', 'FinInstrmGnlAttrbts_ClssfctnTp'])
                    if cfi_col and cfi_col in df.columns:
                        # Filter to requested asset type
                        df = df[df[cfi_col].str[0] == asset_type]
                        self.logger.info(f"  Filtered to {len(df)} records for asset type {asset_type}")
                    
                    if not df.empty:
                        dfs.append(df)
            
            if not dfs:
                return pd.DataFrame()
            
            result = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Retrieved {len(result)} delta records")
            return result
        
        finally:
            # Restore original date range
            self.date_from = original_from
            self.date_to = original_to
    
    def download_file(self, url: str, update: bool = False) -> pd.DataFrame:
        """
        Download and parse a specific FIRDS file by URL.
        
        Args:
            url: Direct download URL
            update: Force re-download
        
        Returns:
            Parsed DataFrame
        
        Example:
            >>> df = downloader.download_file('https://registers.esma.europa.eu/...')
        """
        return self._utils.download_and_parse_file(url, data_type='firds', update=update)
    
    def get_batch_consolidated_data(
        self,
        asset_type: str,
        update: bool = False
    ) -> pd.DataFrame:
        """
        Get consolidated view of all FIRDS files for an asset type.
        
        This method downloads all parts and consolidates them into a single DataFrame,
        removing duplicates and keeping the most recent data.
        
        Args:
            asset_type: CFI first character
            update: Force re-download of files
        
        Returns:
            Consolidated DataFrame with deduplicated instruments
        """
        df = self.get_latest_full_files(asset_type=asset_type, update=update)
        
        # Remove duplicates, keeping most recent based on ISIN
        if 'Id' in df.columns:
            df = df.drop_duplicates(subset=['Id'], keep='last')
        
        self.logger.info(f"Consolidated {len(df)} unique instruments for asset type {asset_type}")
        return df
    
    def cleanup_old_files(
        self,
        asset_type: str,
        keep_count: int = 2,
        file_type: str = 'FULINS'
    ) -> Dict[str, Any]:
        """
        Clean up old cached files, keeping only the most recent ones.
        
        Args:
            asset_type: Asset type to clean up (E, D, C, etc.)
            keep_count: Number of newest files to keep (default: 2)
            file_type: File type pattern to clean ('FULINS' or 'DLTINS')
            
        Returns:
            Dictionary with cleanup statistics
        """
        cache_dir = self.config.downloads_path / 'firds'
        pattern = f"{file_type}_{asset_type}_*_data.csv"
        
        # Find all matching files
        files = list(cache_dir.glob(pattern))
        
        if len(files) <= keep_count:
            return {
                'files_removed': 0,
                'files_kept': len(files),
                'space_freed_mb': 0,
                'message': f'No cleanup needed (found {len(files)} files, keeping {keep_count})'
            }
        
        # Sort by modification time (newest first)
        files_by_time = sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)
        files_to_keep = files_by_time[:keep_count]
        files_to_remove = files_by_time[keep_count:]
        
        # Calculate space that will be freed
        space_freed = sum(f.stat().st_size for f in files_to_remove) / (1024 * 1024)
        
        # Remove old files
        removed_files = []
        for file_path in files_to_remove:
            try:
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                file_path.unlink()
                removed_files.append(file_path.name)
                self.logger.info(f"Removed old file: {file_path.name} ({file_size_mb:.1f} MB)")
            except Exception as e:
                self.logger.warning(f"Failed to remove {file_path.name}: {e}")
        
        kept_files = [f.name for f in files_to_keep]
        
        return {
            'files_removed': len(removed_files),
            'files_kept': len(kept_files),
            'space_freed_mb': space_freed,
            'removed_files': removed_files,
            'kept_files': kept_files,
            'message': f'Cleaned up {len(removed_files)} old files, freed {space_freed:.1f} MB'
        }