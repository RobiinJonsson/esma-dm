"""
FIRDS (Financial Instruments Reference Data System) Client

This module provides access to ESMA's FIRDS dataset, which contains reference data
for financial instruments traded on EU regulated markets.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum

import pandas as pd
import requests

from .utils import Utils
from .config import default_config


class AssetType(Enum):
    """CFI first character representing asset types."""
    COLLECTIVE_INVESTMENT = "C"
    DEBT = "D"
    EQUITY = "E"
    FUTURES = "F"
    RIGHTS = "H"
    OPTIONS = "I"
    STRATEGIES = "J"
    OTHERS = "O"
    REFERENTIAL = "R"
    SWAPS = "S"


class FIRDSClient:
    """
    Client for accessing ESMA FIRDS (Financial Instruments Reference Data System).
    
    FIRDS provides comprehensive reference data for financial instruments including:
    - ISINs and instrument identifiers
    - Instrument classifications (CFI codes)
    - Trading venue information
    - Corporate actions and lifecycle events
    
    Example:
        >>> from esma_dm import FIRDSClient
        >>> 
        >>> # Initialize client
        >>> firds = FIRDSClient(date_from='2024-01-01')
        >>> 
        >>> # Get list of available files
        >>> files = firds.get_file_list()
        >>> 
        >>> # Download latest full equity files
        >>> equity_data = firds.get_latest_full_files(asset_type='E')
        >>> 
        >>> # Get specific ISINs
        >>> instruments = firds.get_instruments(['GB00B1YW4409', 'US0378331005'])
    """
    
    BASE_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
    
    def __init__(
        self,
        date_from: str = "2017-01-01",
        date_to: Optional[str] = None,
        limit: int = 10000,
        config: Optional[Any] = None
    ):
        """
        Initialize FIRDS client.
        
        Args:
            date_from: Start date for filtering files (YYYY-MM-DD format)
            date_to: End date for filtering files (YYYY-MM-DD format, defaults to today)
            limit: Maximum number of records to fetch per request
            config: Optional custom configuration object
        """
        self.date_from = date_from
        self.date_to = date_to or datetime.today().strftime("%Y-%m-%d")
        self.limit = limit
        self.config = config or default_config
        
        self.logger = Utils.set_logger("FIRDSClient")
        self._utils = Utils()
    
    def get_file_list(self) -> pd.DataFrame:
        """
        Retrieve list of available FIRDS files.
        
        Returns:
            DataFrame containing file metadata including:
            - file_name: Name of the file
            - file_type: Type (Full or Delta)
            - publication_date: When file was published
            - download_link: URL to download file
        
        Example:
            >>> firds = FIRDSClient()
            >>> files = firds.get_file_list()
            >>> print(files[['file_name', 'file_type', 'publication_date']])
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
        return self._utils.parse_xml_response(response)
    
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
            update: Force re-download of files
        
        Returns:
            DataFrame containing instrument reference data
        
        Example:
            >>> firds = FIRDSClient()
            >>> 
            >>> # Get all equity instruments
            >>> equities = firds.get_latest_full_files(asset_type='E')
            >>> 
            >>> # Get specific ISINs only
            >>> my_stocks = firds.get_latest_full_files(
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
        
        # Get file list
        files = self.get_file_list()
        
        # Filter for Full files and specific asset type
        pattern = rf"FULINS_{asset_type.upper()}_\d{{8}}_\d+of\d+\.zip"
        files = files[files['file_name'].str.match(pattern, na=False)]
        files = files[files['file_type'] == 'Full']
        
        if files.empty:
            self.logger.warning(f"No files found for asset type {asset_type}")
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
            >>> firds = FIRDSClient()
            >>> instruments = firds.get_instruments([
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
    
    def download_file(self, url: str, update: bool = False) -> pd.DataFrame:
        """
        Download and parse a specific FIRDS file by URL.
        
        Args:
            url: Direct download URL
            update: Force re-download
        
        Returns:
            Parsed DataFrame
        
        Example:
            >>> firds = FIRDSClient()
            >>> df = firds.download_file('https://registers.esma.europa.eu/...')
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
