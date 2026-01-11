"""
FITRS (Financial Instruments Transparency System) Client

This module provides access to ESMA's FITRS dataset, which contains transparency
data for financial instruments including pre-trade and post-trade information.
"""
from datetime import datetime
from typing import List, Optional, Any, Dict
from enum import Enum
from pathlib import Path

import pandas as pd
import requests

from ..utils import Utils
from ..config import default_config
from ..storage.fitrs_store import FITRSStorage
from ..models.transparency_enums import (
    Methodology, InstrumentClassification, FileType as FITRSFileType,
    format_methodology_info, format_classification_info
)


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
        config: Optional[Any] = None,
        db_path: Optional[str] = None
    ):
        """
        Initialize FITRS client.
        
        Args:
            date_from: Start date for filtering files (YYYY-MM-DD format)
            date_to: End date for filtering files (YYYY-MM-DD format, defaults to today)
            limit: Maximum number of records to fetch per request
            config: Optional custom configuration object
            db_path: Path to fitrs.db database file
        """
        self.date_from = date_from
        self.date_to = date_to or datetime.today().strftime("%Y-%m-%d")
        self.limit = limit
        self.config = config or default_config
        
        self.logger = Utils.set_logger("FITRSClient")
        self._utils = Utils()
        
        # Initialize database storage
        self.data_store = FITRSStorage(db_path)
    
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
    
    def index_transparency_data(
        self,
        file_type: str = 'FULECR',
        asset_type: Optional[str] = None,
        latest_only: bool = True
    ) -> Dict[str, Any]:
        """
        Download and index FITRS transparency data into database.
        
        Supports:
        - FULECR: Equity ISIN-level full results
        - FULNCR: Non-equity ISIN-level full results  
        - DLTECR: Equity ISIN-level delta (incremental updates)
        - DLTNCR: Non-equity ISIN-level delta (incremental updates)
        - FULNCR_NYAR: Non-equity sub-class yearly results
        - FULNCR_SISC: Non-equity sub-class SI results
        
        Args:
            file_type: Type of files to index (FULECR, FULNCR, DLTECR, DLTNCR, FULNCR_NYAR, FULNCR_SISC)
            asset_type: Optional asset type filter (for ISIN-level files)
            latest_only: Only process latest files per asset type
            
        Returns:
            Dictionary with processing statistics
            
        Example:
            >>> fitrs = FITRSClient()
            >>> # Index equity ISIN-level full results
            >>> result = fitrs.index_transparency_data('FULECR')
            >>> print(f"Indexed {result['total_instruments']} equity instruments")
            >>> 
            >>> # Index equity delta (incremental updates)
            >>> result = fitrs.index_transparency_data('DLTECR', latest_only=True)
            >>> 
            >>> # Index non-equity sub-class yearly results
            >>> result = fitrs.index_transparency_data('FULNCR_NYAR')
        """
        self.logger.info(f"Indexing {file_type} transparency data")
        
        # Determine if this is sub-class level data
        is_subclass = file_type in ['FULNCR_NYAR', 'FULNCR_SISC']
        
        # Get file list
        files = self.get_file_list()
        
        # Filter files by type
        if is_subclass:
            # Sub-class files: FULNCR_YYYYMMDD_NYAR_<AssetClass>_XofY.zip
            files = files[files['file_name'].str.contains(file_type, na=False)]
        else:
            # ISIN-level files: FULECR_YYYYMMDD_<CFI>_XofY.zip
            files = files[files['file_name'].str.contains(file_type, na=False)]
        
        # Apply asset type filter (only for ISIN-level)
        if asset_type and not is_subclass:
            files = files[files['file_name'].str.contains(f'_{asset_type}_', na=False)]
        
        if latest_only:
            if is_subclass:
                # Sub-class: FULNCR_YYYYMMDD_NYAR_<AssetClass>_XofY.zip
                pattern = rf"{file_type}_(?P<date>\d{{8}})_(?P<asset>\w+)"
            else:
                # ISIN-level: FULECR_YYYYMMDD_E_XofY.zip or DLTECR_YYYYMMDD_E_XofY.zip
                pattern = rf"{file_type}_(?P<date>\d{{8}})_(?P<asset>[A-Z])"
            
            extracted = files['file_name'].str.extract(pattern)
            files['asset'] = extracted['asset']
            files['date'] = extracted['date']
            files = files[files['date'].notna()]  # Filter invalid matches
            
            if not files.empty:
                files = files.sort_values('date').groupby('asset').tail(1)
        
        total_records = 0
        files_processed = 0
        
        for _, file_row in files.iterrows():
            try:
                self.logger.info(f"Processing {file_row['file_name']}")
                df = self.download_file(file_row['download_link'])
                
                if not df.empty:
                    if is_subclass:
                        # Insert into subclass_transparency table
                        count = self.data_store.insert_subclass_transparency_data(df, file_type)
                    else:
                        # Insert into transparency table
                        count = self.data_store.insert_transparency_data(df, file_type)
                    
                    total_records += count
                    files_processed += 1
                    self.logger.info(f"Inserted {count} records")
                
            except Exception as e:
                self.logger.error(f"Error processing {file_row['file_name']}: {e}")
                continue
        
        return {
            'status': 'completed',
            'files_processed': files_processed,
            'total_records': total_records,
            'file_type': file_type,
            'is_subclass': is_subclass
        }
    
    def transparency(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Get transparency data for an ISIN from database.
        
        Args:
            isin: ISIN code
            
        Returns:
            Dictionary with transparency metrics or None if not found
            
        Example:
            >>> fitrs = FITRSClient()
            >>> transparency = fitrs.transparency('GB00B1YW4409')
            >>> print(transparency['liquid_market'])
            >>> print(transparency['average_daily_turnover'])
        """
        return self.data_store.get_transparency(isin)
    
    def query_transparency(
        self,
        liquid_only: bool = False,
        instrument_classification: Optional[str] = None,
        instrument_type: Optional[str] = None,
        min_turnover: Optional[float] = None,
        most_relevant_market: Optional[str] = None,
        methodology: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query ISIN-level transparency data with filters.
        
        Args:
            liquid_only: Only return liquid instruments
            instrument_classification: Filter by classification:
                - SHRS: Shares (common/ordinary shares)
                - DPRS: Depositary Receipts (ADRs, GDRs)
                - ETFS: Exchange Traded Funds
                - OTHR: Other equity instruments
            instrument_type: Filter by type ('equity' or 'non_equity')
            min_turnover: Minimum average daily turnover
            most_relevant_market: Filter by most relevant market ID
            methodology: Filter by methodology:
                - SINT: Systematic Internaliser historical (discontinued April 2024)
                - YEAR: Yearly methodology (12-month rolling)
                - ESTM: Estimation methodology (insufficient data)
                - FFWK: Framework methodology (illiquid pre-trade)
            limit: Maximum number of results
            
        Returns:
            DataFrame with transparency data
            
        Example:
            >>> fitrs = FITRSClient()
            >>> # Get liquid shares with high turnover
            >>> liquid_shares = fitrs.query_transparency(
            ...     liquid_only=True,
            ...     instrument_classification='SHRS',
            ...     min_turnover=1000000
            ... )
            >>> 
            >>> # Get instruments calculated using yearly methodology
            >>> yearly = fitrs.query_transparency(methodology='YEAR')
        """
        sql = "SELECT * FROM transparency WHERE 1=1"
        params = []
        
        if liquid_only:
            sql += " AND liquid_market = TRUE"
        
        if instrument_classification:
            sql += " AND instrument_classification = ?"
            params.append(instrument_classification)
        
        if instrument_type:
            sql += " AND instrument_type = ?"
            params.append(instrument_type)
        
        if min_turnover:
            sql += " AND average_daily_turnover >= ?"
            params.append(min_turnover)
        
        if most_relevant_market:
            sql += " AND most_relevant_market_id = ?"
            params.append(most_relevant_market)
        
        if methodology:
            sql += " AND methodology = ?"
            params.append(methodology)
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.data_store.query(sql, params if params else None)
    
    def query_subclass_transparency(
        self,
        asset_class: Optional[str] = None,
        sub_asset_class_code: Optional[str] = None,
        liquid_only: bool = False,
        methodology: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query sub-class level transparency data with filters.
        
        Args:
            asset_class: Asset class code (e.g., 'BOND', 'DERV')
            sub_asset_class_code: Sub-asset class code
            liquid_only: Only return liquid sub-classes
            methodology: Filter by methodology (YEAR, ESTM, FFWK)
            limit: Maximum number of results
            
        Returns:
            DataFrame with sub-class transparency data
            
        Example:
            >>> fitrs = FITRSClient()
            >>> # Get liquid bond sub-classes
            >>> liquid_bonds = fitrs.query_subclass_transparency(
            ...     asset_class='BOND',
            ...     liquid_only=True
            ... )
        """
        sql = "SELECT * FROM subclass_transparency WHERE 1=1"
        params = []
        
        if asset_class:
            sql += " AND asset_class = ?"
            params.append(asset_class)
        
        if sub_asset_class_code:
            sql += " AND sub_asset_class_code = ?"
            params.append(sub_asset_class_code)
        
        if liquid_only:
            sql += " AND liquid_market = TRUE"
        
        if methodology:
            sql += " AND methodology = ?"
            params.append(methodology)
        
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.data_store.query(sql, params if params else None)
    
    @staticmethod
    def get_methodology_info(code: str) -> dict:
        """
        Get detailed information about a methodology code.
        
        Args:
            code: Methodology code (SINT, YEAR, ESTM, FFWK)
            
        Returns:
            Dictionary with code, description, and validity
            
        Example:
            >>> fitrs = FITRSClient()
            >>> info = fitrs.get_methodology_info('YEAR')
            >>> print(info['description'])
            'Yearly methodology (12-month rolling period)'
        """
        return format_methodology_info(code)
    
    @staticmethod
    def get_classification_info(code: str) -> dict:
        """
        Get detailed information about an instrument classification code.
        
        Args:
            code: Classification code (SHRS, DPRS, ETFS, OTHR)
            
        Returns:
            Dictionary with code, description, and validity
            
        Example:
            >>> fitrs = FITRSClient()
            >>> info = fitrs.get_classification_info('SHRS')
            >>> print(info['description'])
            'Shares (common/ordinary shares)'
        """
        return format_classification_info(code)
    
    @staticmethod
    def list_methodologies() -> list:
        """
        Get list of all available methodology codes with descriptions.
        
        Returns:
            List of dictionaries with code and description
            
        Example:
            >>> fitrs = FITRSClient()
            >>> for m in fitrs.list_methodologies():
            ...     print(f"{m['code']}: {m['description']}")
        """
        return [
            {'code': m.name, 'description': m.value}
            for m in Methodology
        ]
    
    @staticmethod
    def list_classifications() -> list:
        """
        Get list of all instrument classification codes with descriptions.
        
        Returns:
            List of dictionaries with code and description
            
        Example:
            >>> fitrs = FITRSClient()
            >>> for c in fitrs.list_classifications():
            ...     print(f"{c['code']}: {c['description']}")
        """
        return [
            {'code': c.name, 'description': c.value}
            for c in InstrumentClassification
        ]
    
    @staticmethod
    def list_file_types() -> list:
        """
        Get list of all FITRS file types with descriptions.
        
        Returns:
            List of dictionaries with code and description
            
        Example:
            >>> fitrs = FITRSClient()
            >>> for ft in fitrs.list_file_types():
            ...     print(f"{ft['code']}: {ft['description']}")
        """
        return [
            {'code': ft.name, 'description': ft.value}
            for ft in FITRSFileType
        ]
