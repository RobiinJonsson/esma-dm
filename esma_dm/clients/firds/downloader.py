"""
FIRDS file download and management operations.
"""

import logging
from typing import List, Optional, Dict, Any
import pandas as pd
import requests

from .enums import FileType, AssetType
from .models import FIRDSFile
from ...utils import Utils
from ...utils.validators import validate_isin
from ...utils.constants import FIRDS_SOLR_URL


class FIRDSDownloader:
    """Handles downloading and file management for FIRDS data."""
    
    def __init__(self, config, date_from: str, date_to: str, limit: int = 1000):
        """Initialize downloader with configuration."""
        self.config = config
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
            >>> downloader = FIRDSDownloader(config, '2024-01-01', '2024-12-31')
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
        update: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve the latest full FIRDS files for a specific asset type.
        
        Args:
            asset_type: CFI first character (C, D, E, F, H, I, J, O, R, S)
            isin_filter: Optional list of ISINs to filter results
            update: Force re-download of files (default: False, use cached data)
        
        Returns:
            DataFrame containing instrument reference data
        
        Example:
            >>> equities = downloader.get_latest_full_files(asset_type='E')
            >>> my_stocks = downloader.get_latest_full_files(
            ...     asset_type='E',
            ...     isin_filter=['GB00B1YW4409', 'US0378331005']
            ... )
        """
        # Validate asset type
        try:
            AssetType(asset_type)
        except ValueError:
            valid_types = [t.value for t in AssetType]
            raise ValueError(f"Invalid asset_type '{asset_type}'. Must be one of: {valid_types}")
        
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
        return result
    
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