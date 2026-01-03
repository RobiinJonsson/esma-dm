"""
FITRS (Financial Instruments Transparency System) Client

This module provides access to ESMA's FITRS dataset, which contains transparency
data for financial instruments including pre-trade and post-trade information.
"""
from datetime import datetime
from typing import List, Optional, Any
from enum import Enum

import pandas as pd
import requests

from ..utils import Utils
from ..config import default_config


class InstrumentType(Enum):
    """Instrument types for FITRS data."""
    EQUITY = "Equity Instruments"
    NON_EQUITY = "Non-Equity Instruments"


class FITRSClient:
    """
    Client for accessing ESMA FITRS (Financial Instruments Transparency System).
    
    FITRS provides transparency data including:
    - Pre-trade transparency (order book depth, quotes)
    - Post-trade transparency (executed trades)
    - Liquidity assessments
    - Trading venue information
    
    Example:
        >>> from esma_dm import FITRSClient
        >>> 
        >>> # Initialize client
        >>> fitrs = FITRSClient(date_from='2024-01-01')
        >>> 
        >>> # Get list of available files
        >>> files = fitrs.get_file_list()
        >>> 
        >>> # Download latest equity transparency data
        >>> equity_transparency = fitrs.get_latest_full_files(
        ...     asset_type='E',
        ...     instrument_type='equity'
        ... )
        >>> 
        >>> # Get transparency data for specific ISINs
        >>> my_transparency = fitrs.get_instruments(['GB00B1YW4409'])
    """
    
    BASE_URL = "https://registers.esma.europa.eu/solr/esma_registers_fitrs_files/select"
    
    def __init__(
        self,
        date_from: str = "2017-01-01",
        date_to: Optional[str] = None,
        limit: int = 10000,
        config: Optional[Any] = None
    ):
        """
        Initialize FITRS client.
        
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
        
        self.logger = Utils.set_logger("FITRSClient")
        self._utils = Utils()
    
    def get_file_list(self) -> pd.DataFrame:
        """
        Retrieve list of available FITRS files.
        
        Returns:
            DataFrame containing file metadata including:
            - file_name: Name of the file
            - file_type: Type (Full or Delta)
            - creation_date: When file was created
            - download_link: URL to download file
            - instrument_type: Equity or Non-Equity
        
        Example:
            >>> fitrs = FITRSClient()
            >>> files = fitrs.get_file_list()
            >>> print(files[['file_name', 'instrument_type']])
        """
        query_url = (
            f"{self.BASE_URL}?q=*"
            f"&fq=creation_date:[{self.date_from}T00:00:00Z+TO+{self.date_to}T23:59:59Z]"
            f"&wt=xml&indent=true&start=0&rows={self.limit}"
        )
        
        self.logger.info(f"Requesting FITRS file list")
        response = requests.get(query_url)
        
        if response.status_code != 200:
            self.logger.error(f"Request failed with status code {response.status_code}")
            raise Exception(f"Failed to fetch FITRS files: {response.status_code}")
        
        self.logger.info("Request successful, parsing response")
        return self._utils.parse_xml_response(response)
    
    def get_latest_full_files(
        self,
        asset_type: str = "E",
        instrument_type: str = "equity",
        isin_filter: Optional[List[str]] = None,
        update: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve the latest full FITRS transparency files.
        
        Args:
            asset_type: CFI first character (C, D, E, F, H, I, J, O, R, S)
            instrument_type: 'equity' or 'non_equity'
            isin_filter: Optional list of ISINs to filter results
            update: Force re-download of files
        
        Returns:
            DataFrame containing transparency data
        
        Example:
            >>> fitrs = FITRSClient()
            >>> 
            >>> # Get equity transparency data
            >>> eq_transparency = fitrs.get_latest_full_files(
            ...     asset_type='E',
            ...     instrument_type='equity'
            ... )
            >>> 
            >>> # Get debt transparency data
            >>> debt_transparency = fitrs.get_latest_full_files(
            ...     asset_type='D',
            ...     instrument_type='non_equity'
            ... )
        """
        # Validate instrument type
        if instrument_type.lower() not in ['equity', 'non_equity']:
            raise ValueError("instrument_type must be 'equity' or 'non_equity'")
        
        # Get file list
        files = self.get_file_list()
        
        # Filter for Full files
        files = files[files['file_type'] == 'Full']
        
        # Filter by instrument type
        if instrument_type.lower() == 'equity':
            files = files[files['instrument_type'] == InstrumentType.EQUITY.value]
        else:
            files = files[files['instrument_type'] == InstrumentType.NON_EQUITY.value]
        
        # Filter by asset type pattern
        pattern = rf"FUL[EN]CR_\d{{8}}_{asset_type.upper()}_\d+of\d+\.zip"
        files = files[files['file_name'].str.match(pattern, na=False)]
        
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
            df = self._utils.download_and_parse_file(url, data_type='fitrs', update=update)
            
            # Apply ISIN filter if provided
            if isin_filter and 'Id' in df.columns:
                df = df[df['Id'].isin(isin_filter)]
            
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame()
        
        result = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"Retrieved {len(result)} transparency records")
        return result
    
    def get_instruments(
        self,
        isin_list: List[str],
        asset_type: Optional[str] = None,
        instrument_type: str = "equity"
    ) -> pd.DataFrame:
        """
        Retrieve transparency data for specific ISINs.
        
        Args:
            isin_list: List of ISINs to retrieve
            asset_type: Optional asset type filter for efficiency
            instrument_type: 'equity' or 'non_equity'
        
        Returns:
            DataFrame with transparency data for requested ISINs
        
        Example:
            >>> fitrs = FITRSClient()
            >>> transparency = fitrs.get_instruments([
            ...     'GB00B1YW4409',
            ...     'US0378331005'
            ... ])
        """
        if asset_type:
            return self.get_latest_full_files(
                asset_type=asset_type,
                instrument_type=instrument_type,
                isin_filter=isin_list
            )
        
        # Try common asset types
        asset_types = ['E', 'D', 'C']
        all_results = []
        
        for at in asset_types:
            try:
                df = self.get_latest_full_files(
                    asset_type=at,
                    instrument_type=instrument_type,
                    isin_filter=isin_list
                )
                if not df.empty:
                    all_results.append(df)
            except Exception as e:
                self.logger.warning(f"Error fetching {at}: {e}")
                continue
        
        if not all_results:
            self.logger.warning(f"No data found for ISINs: {isin_list}")
            return pd.DataFrame()
        
        return pd.concat(all_results, ignore_index=True)
    
    def download_file(self, url: str, update: bool = False) -> pd.DataFrame:
        """
        Download and parse a specific FITRS file by URL.
        
        Args:
            url: Direct download URL
            update: Force re-download
        
        Returns:
            Parsed DataFrame
        
        Example:
            >>> fitrs = FITRSClient()
            >>> df = fitrs.download_file('https://registers.esma.europa.eu/...')
        """
        return self._utils.download_and_parse_file(url, data_type='fitrs', update=update)
    
    def get_dvcap_latest(self, update: bool = False) -> pd.DataFrame:
        """
        Retrieve the latest DVCAP (Double Volume Cap) files.
        
        DVCAP files contain information about volume cap mechanisms
        for dark pool trading.
        
        Args:
            update: Force re-download of files
        
        Returns:
            DataFrame containing DVCAP data
        
        Example:
            >>> fitrs = FITRSClient()
            >>> dvcap_data = fitrs.get_dvcap_latest()
        """
        # DVCAP uses its own endpoint
        dvcap_url = "https://registers.esma.europa.eu/solr/esma_registers_dvcap_files/select"
        
        query_url = (
            f"{dvcap_url}?q=*"
            f"&fq=creation_date:[{self.date_from}T00:00:00Z+TO+{self.date_to}T23:59:59Z]"
            f"&wt=xml&indent=true&start=0&rows={self.limit}"
        )
        
        self.logger.info(f"Requesting DVCAP file list")
        response = requests.get(query_url)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch DVCAP files: {response.status_code}")
        
        files = self._utils.parse_xml_response(response)
        
        # Extract date and get latest
        pattern = r"DVCRES_(?P<date>\d{8})"
        files['date'] = files['file_name'].str.extract(pattern)['date']
        max_date = files['date'].max()
        latest_file = files[files['date'] == max_date].iloc[0]
        
        self.logger.info(f"Downloading latest DVCAP file from {max_date}")
        return self._utils.download_and_parse_file(
            latest_file['download_link'],
            data_type='fitrs',
            update=update
        )
