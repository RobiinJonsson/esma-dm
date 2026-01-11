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
        db_path: Optional[str] = None,
        mode: str = 'current'
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
            mode: Data mode - 'current' for latest FULINS snapshots or 'history' for version tracking with DLTINS
        """
        self.date_from = date_from
        self.date_to = date_to or datetime.today().strftime("%Y-%m-%d")
        self.limit = limit
        self.config = config or default_config
        self.storage_backend = 'duckdb'  # DuckDB is primary backend
        self.db_path = db_path
        self.mode = mode
        
        if mode not in ['current', 'history']:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'current' or 'history'")
        
        self.logger = Utils.set_logger("FIRDSClient")
        self._utils = Utils()
        self._data_store = None
    
    @property
    def data_store(self) -> StorageBackend:
        """Lazy-load DuckDB data store."""
        if self._data_store is None:
            cache_dir = self.config.downloads_path / 'firds'
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._data_store = DuckDBStorage(cache_dir, db_path=self.db_path, mode=self.mode)
        
        return self._data_store
    
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
            update: Force re-download of files (default: False, use cached data)
        
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
            update: Force re-download of files (default: False, use cached data)
        
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
    
    def process_delta_files(
        self,
        asset_type: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        update: bool = False
    ) -> Dict:
        """
        Download and process delta files with version management.
        
        This method is only available in 'history' mode.
        
        This method:
        1. Downloads DLTINS files for the date range
        2. Extracts record types (NEW, MODIFIED, TERMINATED, CANCELLED)
        3. Applies version management per ESMA Section 8.2
        4. Updates instrument_history table
        
        Args:
            asset_type: CFI first character (C, D, E, F, H, I, J, O, R, S)
            date_from: Start date (defaults to instance date_from)
            date_to: End date (defaults to instance date_to)
            update: Force re-download of files (default: False, use cached data)
        
        Returns:
            Dictionary with processing statistics
        
        Raises:
            ValueError: If called in 'current' mode
        
        Example:
            >>> firds = FIRDSClient(mode='history')
            >>> # Process recent equity deltas
            >>> stats = firds.process_delta_files(
            ...     asset_type='E',
            ...     date_from='2025-01-01',
            ...     date_to='2025-01-31'
            ... )
            >>> print(f"Processed {stats['records_processed']} delta records")
            >>> print(f"New: {stats['new']}, Modified: {stats['modified']}")
        """
        if self.mode != 'history':
            raise ValueError(
                "process_delta_files() is only available in 'history' mode. "
                "Initialize client with FIRDSClient(mode='history')."
            )
        
        self.logger.info(f"Processing delta files for asset type {asset_type}")
        
        # Download delta files
        df = self.get_delta_files(
            asset_type=asset_type,
            date_from=date_from,
            date_to=date_to,
            update=update
        )
        
        if df.empty:
            self.logger.warning("No delta records found")
            return {
                'records_processed': 0,
                'new': 0,
                'modified': 0,
                'terminated': 0,
                'cancelled': 0,
                'errors': 0
            }
        
        # Check if _record_type column exists (from XML parsing)
        if '_record_type' not in df.columns:
            self.logger.error("Delta file missing record type information")
            return {
                'records_processed': 0,
                'new': 0,
                'modified': 0,
                'terminated': 0,
                'cancelled': 0,
                'errors': len(df)
            }
        
        # Extract publication date from filename or use current date
        publication_date = datetime.today().strftime("%Y-%m-%d")
        
        # Process each record
        stats = {
            'records_processed': 0,
            'new': 0,
            'modified': 0,
            'terminated': 0,
            'cancelled': 0,
            'errors': 0
        }
        
        for idx, row in df.iterrows():
            try:
                isin = row.get('Id')
                record_type = row.get('_record_type')
                
                if not isin or not record_type:
                    stats['errors'] += 1
                    continue
                
                # Convert row to dict for record_data
                record_data = {
                    'full_name': row.get('FinInstrmGnlAttrbts_FullNm', row.get('FullNm')),
                    'cfi_code': row.get('FinInstrmGnlAttrbts_ClssfctnTp', row.get('ClssfctnTp')),
                    'issuer': row.get('Issr', ''),
                    'trading_venue_id': row.get('TradgVnId', ''),
                    'cancellation_reason': row.get('CancellationReason', '')
                }
                
                # Process with version management
                result = self.data_store.process_delta_record(
                    isin=isin,
                    record_type=record_type,
                    record_data=record_data,
                    publication_date=publication_date,
                    source_file=f"DLTINS_{asset_type}_{publication_date}"
                )
                
                stats['records_processed'] += 1
                if result['status'] == 'inserted':
                    stats['new'] += 1
                elif result['status'] == 'updated':
                    stats['modified'] += 1
                elif result['status'] == 'terminated':
                    stats['terminated'] += 1
                elif result['status'] == 'cancelled':
                    stats['cancelled'] += 1
                else:
                    stats['errors'] += 1
                    
            except Exception as e:
                self.logger.error(f"Error processing record {idx}: {e}")
                stats['errors'] += 1
        
        self.logger.info(f"Delta processing complete: {stats['records_processed']} records processed")
        self.logger.info(f"  NEW: {stats['new']}, MODIFIED: {stats['modified']}, "
                        f"TERMINATED: {stats['terminated']}, CANCELLED: {stats['cancelled']}, "
                        f"ERRORS: {stats['errors']}")
        
        return stats
    
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
    
    def index_cached_files(
        self,
        asset_type: Optional[str] = None,
        latest_only: bool = True,
        file_type: str = 'FULINS',
        delete_csv: bool = False
    ) -> Dict:
        """
        Index downloaded CSV files into the database.
        
        Loads FIRDS data from cached CSV files into DuckDB. Supports filtering by
        asset type and automatically selecting latest files when multiple versions exist.
        
        Args:
            asset_type: Filter by asset type (C, D, E, F, H, I, J, O, R, S) or None for all
            latest_only: If True, only index the most recent files for each asset type (default: True)
            file_type: Filter by file type - 'FULINS' for snapshots (default), 'DLTINS' for deltas
            delete_csv: Delete CSV files after successful indexing (default: False)
        
        Returns:
            Dictionary with indexing statistics including:
            - total_instruments: Total instruments indexed
            - total_listings: Total venue listings indexed
            - files_processed: Number of files processed
            - files_skipped: Number of files skipped
            - failed_files: List of files that failed to index
            - asset_types_processed: List of asset types that were indexed
        
        Example:
            >>> firds = FIRDSClient()
            >>> 
            >>> # Index all latest FULINS files
            >>> stats = firds.index_cached_files()
            >>> 
            >>> # Index only equities
            >>> stats = firds.index_cached_files(asset_type='E')
            >>> 
            >>> # Index all equity files (not just latest)
            >>> stats = firds.index_cached_files(asset_type='E', latest_only=False)
            >>> 
            >>> # Index and delete CSV files
            >>> stats = firds.index_cached_files(delete_csv=True)
        """
        cache_dir = self.config.downloads_path / 'firds'
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Build file pattern based on filters
        if asset_type:
            # Validate asset type
            valid_types = ['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S']
            if asset_type not in valid_types:
                raise ValueError(f"Invalid asset_type: {asset_type}. Must be one of {valid_types}")
            pattern = f"{file_type}_{asset_type}_*_data.csv"
        else:
            pattern = f"{file_type}_*_data.csv"
        
        self.logger.info(f"Scanning for files matching pattern: {pattern}")
        csv_files = sorted(cache_dir.glob(pattern))
        
        if not csv_files:
            self.logger.warning(f"No files found matching pattern: {pattern}")
            return {
                'total_instruments': 0,
                'total_listings': 0,
                'files_processed': 0,
                'files_skipped': 0,
                'failed_files': [],
                'asset_types_processed': []
            }
        
        self.logger.info(f"Found {len(csv_files)} files matching pattern")
        
        # If latest_only, filter to most recent file per asset type
        files_to_process = csv_files
        files_skipped = 0
        
        if latest_only:
            from collections import defaultdict
            
            # Group files by asset type
            files_by_type = defaultdict(list)
            for f in csv_files:
                # Extract asset type from filename (e.g., FULINS_E_20260103_...)
                parts = f.name.split('_')
                if len(parts) >= 3:
                    atype = parts[1]  # Second part is asset type
                    files_by_type[atype].append(f)
            
            # Keep only the latest file per asset type (by filename, which includes date)
            files_to_process = []
            for atype, files in files_by_type.items():
                latest = max(files, key=lambda f: f.name)  # Latest by filename sort
                files_to_process.append(latest)
                files_skipped += len(files) - 1
                if len(files) > 1:
                    self.logger.info(f"Asset type {atype}: using latest file {latest.name} (skipped {len(files)-1} older files)")
            
            files_to_process = sorted(files_to_process)
            self.logger.info(f"After latest_only filter: {len(files_to_process)} files to process, {files_skipped} skipped")
        
        # Index selected files
        total_instruments = 0
        total_listings = 0
        files_processed = 0
        failed_files = []
        asset_types_processed = set()
        
        for i, csv_file in enumerate(files_to_process, 1):
            try:
                self.logger.info(f"[{i}/{len(files_to_process)}] Indexing {csv_file.name}")
                
                # index_csv_file returns int (instrument count)
                count = self.data_store.index_csv_file(csv_file)
                
                total_instruments += count
                files_processed += 1
                
                # Extract asset type from filename
                parts = csv_file.name.split('_')
                if len(parts) >= 2:
                    asset_types_processed.add(parts[1])
                
                # Get listings count from database
                try:
                    listings_count = self.data_store.con.execute(
                        "SELECT COUNT(*) FROM listings WHERE source_file = ?", 
                        [csv_file.name]
                    ).fetchone()[0]
                    total_listings += listings_count
                    self.logger.info(f"  Indexed {count:,} instruments, {listings_count:,} listings")
                except Exception:
                    self.logger.info(f"  Indexed {count:,} instruments")
                
                # Delete CSV file after successful processing if requested
                if delete_csv:
                    csv_file.unlink()
                    self.logger.info(f"  Deleted {csv_file.name}")
                    
            except Exception as e:
                self.logger.error(f"  Failed to index {csv_file.name}: {e}")
                failed_files.append(csv_file.name)
        
        self.logger.info(f"Completed: Indexed {files_processed} files with {total_instruments:,} instruments and {total_listings:,} listings")
        
        if failed_files:
            self.logger.warning(f"Failed to index {len(failed_files)} files: {failed_files}")
        
        return {
            'total_instruments': total_instruments,
            'total_listings': total_listings,
            'files_processed': files_processed,
            'files_skipped': files_skipped,
            'failed_files': failed_files,
            'asset_types_processed': sorted(list(asset_types_processed))
        }
    
    def index_unloaded_fulins(self) -> Dict:
        """
        Index all FULINS files from cache that haven't been loaded yet.
        
        Scans the cache directory for FULINS CSV files, checks which ones
        have already been indexed in the database, and indexes the remaining files.
        
        Returns:
            Dictionary with indexing statistics including:
            - files_found: Total FULINS files in cache
            - files_already_indexed: Files already in database
            - files_indexed: Newly indexed files
            - total_instruments: Total instruments indexed
            - failed_files: List of files that failed to index
        
        Example:
            >>> firds = FIRDSClient()
            >>> stats = firds.index_unloaded_fulins()
            >>> print(f"Found {stats['files_found']} FULINS files")
            >>> print(f"Indexed {stats['files_indexed']} new files")
            >>> print(f"Added {stats['total_instruments']:,} instruments")
        """
        cache_dir = self.config.downloads_path / 'firds'
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info("Scanning for unloaded FULINS files...")
        
        # Find all FULINS CSV files
        fulins_files = sorted(cache_dir.glob("FULINS_*_data.csv"))
        self.logger.info(f"Found {len(fulins_files)} FULINS files in cache")
        
        if not fulins_files:
            self.logger.warning("No FULINS files found in cache")
            return {
                'files_found': 0,
                'files_already_indexed': 0,
                'files_indexed': 0,
                'total_instruments': 0,
                'failed_files': []
            }
        
        # Get list of already indexed files
        indexed_files = set()
        try:
            result = self.data_store.con.execute("""
                SELECT DISTINCT source_file 
                FROM instruments
            """).fetchall()
            indexed_files = {row[0] for row in result}
            self.logger.info(f"Found {len(indexed_files)} files already indexed")
        except Exception as e:
            self.logger.warning(f"Could not query indexed files: {e}")
        
        # Filter to unindexed files
        unindexed_files = [f for f in fulins_files if f.name not in indexed_files]
        
        if not unindexed_files:
            self.logger.info("All FULINS files are already indexed")
            return {
                'files_found': len(fulins_files),
                'files_already_indexed': len(fulins_files),
                'files_indexed': 0,
                'total_instruments': 0,
                'failed_files': []
            }
        
        self.logger.info(f"Indexing {len(unindexed_files)} new FULINS files...")
        
        # Index unindexed files
        total_instruments = 0
        failed_files = []
        
        for i, csv_file in enumerate(unindexed_files, 1):
            try:
                self.logger.info(f"[{i}/{len(unindexed_files)}] Indexing {csv_file.name}")
                count = self.data_store.index_csv_file(csv_file)
                total_instruments += count
                self.logger.info(f"  Indexed {count:,} instruments")
            except Exception as e:
                self.logger.error(f"  Failed to index {csv_file.name}: {e}")
                failed_files.append(csv_file.name)
        
        self.logger.info(f"Completed: Indexed {len(unindexed_files) - len(failed_files)} files with {total_instruments:,} instruments")
        
        if failed_files:
            self.logger.warning(f"Failed to index {len(failed_files)} files: {failed_files}")
        
        return {
            'files_found': len(fulins_files),
            'files_already_indexed': len(indexed_files),
            'files_indexed': len(unindexed_files) - len(failed_files),
            'total_instruments': total_instruments,
            'failed_files': failed_files
        }
    
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

