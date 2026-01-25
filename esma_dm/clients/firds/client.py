"""
Main FIRDS client class that composes all modular components.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
import pandas as pd

from .enums import FileType, AssetType
from .models import FIRDSFile
from .downloader import FIRDSDownloader
from .parser import FIRDSParser
from .delta_processor import FIRDSDeltaProcessor
from ...config import default_config
from ...storage import StorageBackend, DuckDBStorage


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
        >>> # Download and index latest equity data
        >>> firds.get_latest_full_files(asset_type='E')
        >>> firds.index_cached_files(asset_type='E')
        >>> 
        >>> # Query specific instrument
        >>> apple = firds.reference('US0378331005')
        >>> print(f"Name: {apple['full_name']}")
    """
    
    def __init__(
        self,
        date_from: str = '2024-01-01',
        date_to: Optional[str] = None,
        limit: int = 1000,
        config=None,
        db_path: Optional[str] = None,
        mode: str = 'current'
    ):
        """
        Initialize FIRDS client.
        
        Args:
            date_from: Start date for file queries (YYYY-MM-DD format)
            date_to: End date for file queries (defaults to today)
            limit: Maximum number of results per query
            config: Configuration object (uses default if None)
            db_path: Custom database path (optional)
            mode: Database mode ('current' or 'history')
        """
        self.config = config or default_config
        self.date_from = date_from
        self.date_to = date_to or datetime.today().strftime('%Y-%m-%d')
        self.limit = limit
        self.db_path = db_path
        self.mode = mode
        
        self.logger = logging.getLogger(__name__)
        
        # Lazy-loaded storage
        self._data_store = None
        
        # Initialize modular components
        self.downloader = FIRDSDownloader(
            config=self.config,
            date_from=self.date_from,
            date_to=self.date_to,
            limit=self.limit
        )
        
        # Parser and delta processor will be initialized with data store when needed
        self._parser = None
        self._delta_processor = None
    
    @property
    def data_store(self) -> DuckDBStorage:
        """Lazy-load DuckDB data store."""
        if self._data_store is None:
            cache_dir = self.config.downloads_path / 'firds'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Get proper database path from config
            db_path = self.db_path or str(self.config.get_database_path('firds', self.mode))
            
            self._data_store = DuckDBStorage(cache_dir, db_path=db_path, mode=self.mode)
        
        return self._data_store
    
    @property
    def parser(self) -> FIRDSParser:
        """Lazy-load parser with data store."""
        if self._parser is None:
            self._parser = FIRDSParser(self.config, self.data_store)
        return self._parser
    
    @property
    def delta_processor(self) -> FIRDSDeltaProcessor:
        """Lazy-load delta processor with data store and downloader."""
        if self._delta_processor is None:
            self._delta_processor = FIRDSDeltaProcessor(
                self.config, self.data_store, self.downloader
            )
        return self._delta_processor
    
    # Download methods - delegated to downloader
    def get_file_list(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> pd.DataFrame:
        """Retrieve list of available FIRDS files."""
        return self.downloader.get_file_list(file_type=file_type, asset_type=asset_type)
    
    def get_files_metadata(
        self,
        file_type: Optional[str] = None,
        asset_type: Optional[str] = None
    ) -> List[FIRDSFile]:
        """Get structured metadata for available FIRDS files."""
        return self.downloader.get_files_metadata(file_type=file_type, asset_type=asset_type)
    
    def get_latest_full_files(
        self,
        asset_type: str = "E",
        isin_filter: Optional[List[str]] = None,
        update: bool = False
    ) -> pd.DataFrame:
        """Retrieve the latest full FIRDS files for a specific asset type."""
        return self.downloader.get_latest_full_files(
            asset_type=asset_type, isin_filter=isin_filter, update=update
        )
    
    def get_instruments(
        self,
        isin_list: List[str],
        asset_type: Optional[str] = None
    ) -> pd.DataFrame:
        """Retrieve reference data for specific ISINs."""
        return self.downloader.get_instruments(isin_list=isin_list, asset_type=asset_type)
    
    def get_delta_files(
        self,
        asset_type: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        update: bool = False
    ) -> pd.DataFrame:
        """Retrieve delta (incremental) FIRDS files for a specific asset type."""
        return self.downloader.get_delta_files(
            asset_type=asset_type, date_from=date_from, date_to=date_to, update=update
        )
    
    def download_file(self, url: str, update: bool = False) -> pd.DataFrame:
        """Download and parse a specific FIRDS file by URL."""
        return self.downloader.download_file(url=url, update=update)
    
    def get_batch_consolidated_data(
        self,
        asset_type: str,
        update: bool = False
    ) -> pd.DataFrame:
        """Get consolidated view of all FIRDS files for an asset type."""
        return self.downloader.get_batch_consolidated_data(asset_type=asset_type, update=update)
    
    # Parser methods - delegated to parser
    def reference(self, isin: str) -> Optional[pd.Series]:
        """Retrieve reference data for a single ISIN."""
        return self.parser.reference(isin)
    
    def index_cached_files(
        self,
        asset_type: Optional[str] = None,
        latest_only: bool = True,
        file_type: str = 'FULINS',
        delete_csv: bool = False
    ) -> Dict:
        """Index downloaded CSV files into the database with mode validation."""
        # Mode-based file type validation
        if self.mode == 'current' and file_type != 'FULINS':
            raise ValueError(f"Current mode only supports FULINS files, not {file_type}. Use history mode for DLTINS processing.")
        
        # Check if appropriate files exist
        cache_dir = self.config.downloads_path / 'firds'
        if asset_type:
            pattern = f"{file_type}_{asset_type}_*_data.csv"
        else:
            pattern = f"{file_type}_*_data.csv"
        
        existing_files = list(cache_dir.glob(pattern))
        
        if not existing_files and file_type == 'FULINS':
            self.logger.warning(f"No FULINS files found for asset_type={asset_type}. Downloading latest files...")
            try:
                self.get_latest_full_files(asset_type=asset_type or 'E', update=True)
                self.logger.info("Download completed. Proceeding with indexing...")
            except Exception as e:
                self.logger.error(f"Failed to download FULINS files: {e}")
                return {
                    'total_instruments': 0,
                    'total_listings': 0,
                    'files_processed': 0,
                    'files_skipped': 0,
                    'failed_files': [f"Download failed: {e}"],
                    'asset_types_processed': []
                }
        
        return self.parser.index_cached_files(
            asset_type=asset_type, latest_only=latest_only, 
            file_type=file_type, delete_csv=delete_csv
        )
    
    def index_unloaded_fulins(self) -> Dict:
        """Index all FULINS files from cache that haven't been loaded yet."""
        return self.parser.index_unloaded_fulins()
    
    # Validation methods - delegated to parser (for backward compatibility)
    @staticmethod
    def validate_isin(isin: str) -> bool:
        """Validate ISIN format (ISO 6166). Deprecated: Use esma_dm.utils.validators directly."""
        return FIRDSParser.validate_isin(isin)
    
    @staticmethod
    def validate_lei(lei: str) -> bool:
        """Validate LEI format (ISO 17442). Deprecated: Use esma_dm.utils.validators directly."""
        return FIRDSParser.validate_lei(lei)
    
    @staticmethod
    def validate_cfi(cfi: str) -> bool:
        """Validate CFI code format (ISO 10962). Deprecated: Use esma_dm.utils.validators directly."""
        return FIRDSParser.validate_cfi(cfi)
    
    # Delta processing methods - delegated to delta processor (history mode only)
    def process_delta_files(
        self,
        asset_type: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        update: bool = False
    ) -> Dict:
        """Download and process delta files with version management (history mode only)."""
        if self.mode != 'history':
            raise ValueError(f"Delta file processing is only available in history mode, not {self.mode} mode.")
        
        return self.delta_processor.process_delta_files(
            asset_type=asset_type, date_from=date_from, date_to=date_to, update=update
        )
    
    # Statistics methods - delegated to data store
    def get_store_stats(self) -> Dict:
        """Get statistics about the data store."""
        return self.data_store.get_stats()
    
    def get_asset_type_breakdown(self) -> pd.DataFrame:
        """Get aggregated statistics by asset type."""
        # Get raw counts from data store
        stats_dict = self.data_store.get_stats_by_asset_type()
        
        if not stats_dict:
            return pd.DataFrame(columns=['asset_type', 'asset_name', 'count', 'percentage'])
        
        # Asset type mappings
        asset_names = {
            'E': 'Equities',
            'D': 'Debt Instruments', 
            'C': 'Collective Investment Vehicles',
            'R': 'Entitlements (Rights)',
            'O': 'Options',
            'F': 'Futures',
            'S': 'Swaps',
            'H': 'Non-Listed Complex Derivatives',
            'I': 'Spot Commodities',
            'J': 'Forwards'
        }
        
        # Calculate total for percentages
        total_count = sum(stats_dict.values())
        
        # Create DataFrame
        data = []
        for asset_type, count in stats_dict.items():
            data.append({
                'asset_type': asset_type,
                'asset_name': asset_names.get(asset_type, f'Unknown ({asset_type})'),
                'count': count,
                'percentage': (count / total_count * 100) if total_count > 0 else 0
            })
        
        return pd.DataFrame(data).sort_values('count', ascending=False)