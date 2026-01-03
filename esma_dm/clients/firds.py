"""
FIRDS (Financial Instruments Reference Data System) Client

This module provides access to ESMA's FIRDS dataset, which contains reference data
for financial instruments traded on EU regulated markets.

Reference: RTS 23 - Commission Delegated Regulation (EU) supplementing MiFIR
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass

import pandas as pd
import requests

from ..utils import Utils
from ..config import default_config
from ..storage import StorageBackend, DuckDBStorage


class FileType(Enum):
    """FIRDS file types."""
    FULINS = "FULINS"  # Full instrument snapshot
    DLTINS = "DLTINS"  # Delta (incremental updates)


class AssetType(Enum):
    """CFI first character representing asset types (ISO 10962)."""
    COLLECTIVE_INVESTMENT = "C"  # Collective investment vehicles
    DEBT = "D"  # Debt instruments (bonds, notes)
    EQUITY = "E"  # Equities (shares, units)
    FUTURES = "F"  # Futures
    RIGHTS = "H"  # Rights, warrants
    OPTIONS = "I"  # Options
    STRATEGIES = "J"  # Strategies, multi-leg
    OTHERS = "O"  # Others (misc)
    REFERENTIAL = "R"  # Referential instruments
    SWAPS = "S"  # Swaps


class CommodityBaseProduct(Enum):
    """Commodity base product classifications."""
    AGRI = "AGRI"  # Agricultural
    NRGY = "NRGY"  # Energy
    ENVR = "ENVR"  # Environmental
    EMIS = "EMIS"  # Emissions
    FRGT = "FRGT"  # Freight
    FRTL = "FRTL"  # Fertilizer
    INDP = "INDP"  # Industrial Products
    METL = "METL"  # Metals
    POLY = "POLY"  # Polypropylene / Plastics
    INFL = "INFL"  # Inflation
    OEST = "OEST"  # Official Economic Statistics
    OTHR = "OTHR"  # Other


class OptionType(Enum):
    """Option type classifications."""
    CALL = "CALL"  # Call option
    PUT = "PUT"   # Put option
    OTHR = "OTHR"  # Other


class ExerciseStyle(Enum):
    """Option exercise style."""
    EURO = "EURO"  # European (exercise only at expiry)
    AMER = "AMER"  # American (exercise anytime)
    BRMN = "BRMN"  # Bermudan (exercise at specific dates)
    ASIA = "ASIA"  # Asian (average price)


class DeliveryType(Enum):
    """Settlement delivery type."""
    PHYS = "PHYS"  # Physical delivery
    CASH = "CASH"  # Cash settlement
    OPTL = "OPTL"  # Optional (choice)


class BondSeniority(Enum):
    """Bond seniority classifications."""
    SNDB = "SNDB"  # Senior debt
    MZZD = "MZZD"  # Mezzanine
    SBOD = "SBOD"  # Subordinated
    JUND = "JUND"  # Junior


@dataclass
class FIRDSFile:
    """Metadata for a FIRDS file."""
    file_name: str
    file_type: str  # "Full" or "Delta"
    publication_date: str
    download_link: str
    asset_type: Optional[str] = None
    date_extracted: Optional[str] = None
    part_number: Optional[int] = None
    total_parts: Optional[int] = None
    
    @classmethod
    def from_row(cls, row: pd.Series) -> 'FIRDSFile':
        """Create FIRDSFile from DataFrame row."""
        file_name = row.get('file_name', '')
        
        # Extract asset type and date from filename
        # Format: FULINS_E_20240101_1of2.zip or DLTINS_D_20240101_1of1.zip
        asset_type = None
        date_extracted = None
        part_number = None
        total_parts = None
        
        if '_' in file_name:
            parts = file_name.replace('.zip', '').split('_')
            if len(parts) >= 3:
                asset_type = parts[1]
                date_extracted = parts[2]
                if len(parts) >= 4 and 'of' in parts[3]:
                    part_info = parts[3].split('of')
                    part_number = int(part_info[0]) if part_info[0].isdigit() else None
                    total_parts = int(part_info[1]) if len(part_info) > 1 and part_info[1].isdigit() else None
        
        return cls(
            file_name=file_name,
            file_type=row.get('file_type', ''),
            publication_date=row.get('publication_date', ''),
            download_link=row.get('download_link', ''),
            asset_type=asset_type,
            date_extracted=date_extracted,
            part_number=part_number,
            total_parts=total_parts
        )


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
        config: Optional[Any] = None,
        storage_backend: str = 'duckdb',
        db_path: Optional[str] = None
    ):
        """
        Initialize FIRDS client.
        
        Args:
            date_from: Start date for filtering files (YYYY-MM-DD format)
            date_to: End date for filtering files (YYYY-MM-DD format, defaults to today)
            limit: Maximum number of records to fetch per request
            config: Optional custom configuration object
            storage_backend: Storage backend to use ('json' or 'duckdb', default: 'duckdb')
            db_path: Path to database file (for duckdb) or cloud connection string
        """
        self.date_from = date_from
        self.date_to = date_to or datetime.today().strftime("%Y-%m-%d")
        self.limit = limit
        self.config = config or default_config
        self.storage_backend = 'duckdb'  # DuckDB is primary backend
        self.db_path = db_path
        
        self.logger = Utils.set_logger("FIRDSClient")
        self._utils = Utils()
        self._data_store = None
    
    @property
    def data_store(self) -> StorageBackend:
        """Lazy-load DuckDB data store."""
        if self._data_store is None:
            cache_dir = self.config.downloads_path / 'firds'
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._data_store = DuckDBStorage(cache_dir, db_path=self.db_path)
        
        return self._data_store
    
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
            >>> firds = FIRDSClient()
            >>> 
            >>> # Get all files
            >>> files = firds.get_file_list()
            >>> 
            >>> # Get only FULINS files for equities
            >>> equity_fulins = firds.get_file_list(file_type='FULINS', asset_type='E')
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
            pattern = rf"{file_type or '[A-Z]+'}_{{asset_type_upper}}_"
            df = df[df['file_name'].str.match(pattern, na=False)]
        
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
            >>> firds = FIRDSClient()
            >>> files = firds.get_files_metadata(file_type='FULINS', asset_type='E')
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
        
        # Get file list with filters
        files = self.get_file_list(file_type='FULINS', asset_type=asset_type)
        files = files[files['file_type'] == 'Full']
        
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
    
    def reference(self, isin: str) -> Optional[pd.Series]:
        """
        Retrieve reference data for a single ISIN.
        
        Searches the local data store (JSON file with normalized data).
        Data store is automatically populated when files are downloaded.
        
        Args:
            isin: ISIN code to retrieve
        
        Returns:
            Series containing reference data for the instrument, or None if not found
        
        Raises:
            ValueError: If ISIN format is invalid
        
        Example:
            >>> from esma_dm import FIRDSClient
            >>> 
            >>> firds = FIRDSClient()
            >>> ref = firds.reference('US0378331005')
            >>> 
            >>> if ref is not None:
            ...     print(f"Name: {ref.get('full_name')}")
            ...     print(f"CFI: {ref.get('cfi_code')}")
            ...     print(f"Currency: {ref.get('currency')}")
        """
        # Validate ISIN format
        if not self.validate_isin(isin):
            raise ValueError(f"Invalid ISIN format: {isin}")
        
        # Get from data store
        instrument_data = self.data_store.get_instrument(isin)
        
        if instrument_data:
            # Convert dict to Series
            return pd.Series(instrument_data)
        
        self.logger.warning(f"No reference data found for ISIN: {isin}")
        self.logger.info("Try downloading files first or use index_cached_files()")
        return None
    
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
            update: Force re-download of files
        
        Returns:
            DataFrame containing instrument changes
        
        Example:
            >>> firds = FIRDSClient()
            >>> 
            >>> # Get all equity changes in date range
            >>> changes = firds.get_delta_files(
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
            
            # Get file list with filters
            files = self.get_file_list(file_type='DLTINS', asset_type=asset_type)
            files = files[files['file_type'] == 'Delta']
            
            if files.empty:
                self.logger.warning(f"No DLTINS files found for asset type {asset_type}")
                return pd.DataFrame()
            
            self.logger.info(f"Found {len(files)} delta files for asset type {asset_type}")
            
            # Download and parse files
            dfs = []
            for url in files['download_link'].unique():
                self.logger.info(f"Downloading and parsing {url}")
                df = self._utils.download_and_parse_file(url, data_type='firds', update=update)
                
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
    @staticmethod
    def validate_isin(isin: str) -> bool:
        """
        Validate ISIN format (ISO 6166).
        
        Args:
            isin: ISIN code to validate
        
        Returns:
            True if valid ISIN format
        
        Example:
            >>> FIRDSClient.validate_isin('US0378331005')
            True
            >>> FIRDSClient.validate_isin('INVALID')
            False
        """
        if not isinstance(isin, str) or len(isin) != 12:
            return False
        
        # First 2 chars: country code (letters)
        if not isin[:2].isalpha():
            return False
        
        # Next 9 chars: alphanumeric
        if not isin[2:11].isalnum():
            return False
        
        # Last char: check digit (numeric)
        if not isin[11].isdigit():
            return False
        
        return True
    
    @staticmethod
    def validate_lei(lei: str) -> bool:
        """
        Validate LEI format (ISO 17442).
        
        Args:
            lei: LEI code to validate
        
        Returns:
            True if valid LEI format
        
        Example:
            >>> FIRDSClient.validate_lei('549300VALTPVHYSYMH70')
            True
        """
        if not isinstance(lei, str) or len(lei) != 20:
            return False
        
        # All characters must be alphanumeric
        if not lei.isalnum():
            return False
        
        return True
    
    @staticmethod
    def validate_cfi(cfi: str) -> bool:
        """
        Validate CFI code format (ISO 10962).
        
        Args:
            cfi: CFI code to validate
        
        Returns:
            True if valid CFI format
        
        Example:
            >>> FIRDSClient.validate_cfi('ESVUFR')
            True
        """
        if not isinstance(cfi, str) or len(cfi) != 6:
            return False
        
        # All characters must be letters
        if not cfi.isalpha():
            return False
        
        # First character must be valid asset type
        valid_first_chars = [at.value for at in AssetType]
        if cfi[0] not in valid_first_chars:
            return False
        
        return True
    
    def index_cached_files(self, delete_csv: bool = True) -> Dict:
        """
        Index all downloaded CSV files into the JSON data store.
        
        Parses CSV files using instrument data models (Equity, Debt, Derivative)
        and stores normalized data in JSON format for fast lookups. Optionally
        deletes CSV files after successful indexing to save disk space.
        
        Args:
            delete_csv: Delete CSV files after successful indexing (default True)
        
        Returns:
            Dictionary with indexing statistics
        
        Example:
            >>> firds = FIRDSClient()
            >>> stats = firds.index_cached_files(delete_csv=True)
            >>> print(f"Indexed {stats['files_processed']} files")
            >>> print(f"Total instruments: {stats['total_instruments']}")
        """
        cache_dir = self.config.downloads_path / 'firds'
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("Indexing all cached CSV files...")
        results = self.data_store.index_all_csv_files(cache_dir, delete_csv=delete_csv)
        
        self.logger.info(f"Indexed {results['files_processed']} files")
        self.logger.info(f"Total instruments: {results['total_instruments']}")
        
        if results.get('failed_files'):
            self.logger.warning(f"Failed to index {len(results['failed_files'])} files")
        
        return results
    
    def get_store_stats(self) -> Dict:
        """
        Get statistics about the data store.
        
        Returns:
            Dictionary with store statistics including total instruments,
            files processed, and storage size
        
        Example:
            >>> firds = FIRDSClient()
            >>> stats = firds.get_store_stats()
            >>> print(f"Total instruments: {stats['total_instruments']}")
            >>> print(f"Files processed: {stats['files_processed']}")
            >>> print(f"Store size: {stats['store_size_mb']:.2f} MB")
        """
        return self.data_store.get_stats()
    
    def get_stats_by_asset_type(self) -> pd.DataFrame:
        """
        Get aggregated statistics by asset type.
        
        Returns breakdown of instruments by CFI asset type with counts and percentages.
        Asset types follow ISO 10962 classification:
        - E: Equities
        - D: Debt Instruments  
        - C: Collective Investment Vehicles
        - R: Entitlements (Rights)
        - O: Options
        - F: Futures
        - S: Swaps
        - H: Non-Listed Complex Derivatives
        - I: Spot Commodities
        - J: Forwards
        
        Returns:
            DataFrame with columns: asset_type, asset_name, count, percentage
            
        Example:
            >>> firds = FIRDSClient()
            >>> stats = firds.get_stats_by_asset_type()
            >>> print(stats)
            asset_type           asset_name  count  percentage
                     E             Equities  50000       25.50
                     D    Debt Instruments  45000       22.95
        """
        return self.data_store.get_stats_by_asset_type()

