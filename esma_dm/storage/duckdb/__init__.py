"""
DuckDB storage backend with vectorized bulk loading - modular implementation.

This module provides a unified interface to the modular DuckDB storage components.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any

from .connection import DuckDBConnection
from .operations import DuckDBOperations  
from .queries import DuckDBQueries
from .versioning import DuckDBVersioning
from ..base import StorageBackend


class DuckDBStorage(StorageBackend):
    """
    DuckDB storage with star schema and vectorized bulk loading.
    
    Composed of modular components:
    - Connection: Database initialization and management
    - Operations: Bulk insert and update operations
    - Queries: Instrument retrieval and search
    - Versioning: Delta processing (history mode only)
    """
    
    def __init__(self, cache_dir: Path, db_path: str, mode: str = 'current'):
        """Initialize DuckDB storage with modular components."""
        super().__init__(cache_dir)
        self.logger = logging.getLogger(__name__)
        
        # Initialize connection manager with database path
        self.connection = DuckDBConnection(db_path, mode)
        
        # Initialize modular components
        self.operations = DuckDBOperations(self.connection)
        self.queries = DuckDBQueries(self.connection)
        
        # Only initialize versioning for history mode
        self.versioning = DuckDBVersioning(self.connection) if mode == 'history' else None
    
    @property
    def con(self):
        """Get database connection."""
        return self.connection.con
    
    @property
    def mode(self):
        """Get database mode."""
        return self.connection.mode
    
    @property
    def db_path(self):
        """Get database path."""
        return self.connection.db_path
    
    # Connection methods
    def initialize(self, mode: Optional[str] = None, verify_only: bool = False):
        """Initialize database schema."""
        return self.connection.initialize(mode, verify_only)
    
    def drop(self, confirm: bool = False):
        """Drop the database."""
        return self.connection.drop(confirm)
    
    def close(self):
        """Close database connection."""
        self.connection.close()
    
    # Operations methods
    def update(self, asset_type: Optional[str] = None):
        """Update database from cached CSV files."""
        return self.operations.update(asset_type)
    
    def index_csv_file(self, csv_path: Path) -> int:
        """Index a single CSV file."""
        return self.operations.index_csv_file(csv_path)
    
    def index_all_csv_files(self, csv_dir: Path, pattern: str = "*.csv", delete_csv: bool = False) -> dict:
        """Index all CSV files in a directory."""
        return self.operations.index_all_csv_files(csv_dir, pattern, delete_csv)
    
    # Query methods
    def get_instrument(self, isin: str) -> Optional[Dict[str, Any]]:
        """Get instrument by ISIN."""
        return self.queries.get_instrument(isin)
    
    def get_stats_by_asset_type(self) -> Dict[str, int]:
        """Get instrument count by asset type."""
        return self.queries.get_stats_by_asset_type()
    
    def get_instrument_history(self, isin: str) -> List[Dict[str, Any]]:
        """Get version history for an instrument (history mode only)."""
        return self.queries.get_instrument_history(isin)
    
    def search_instruments(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search instruments by name or ISIN."""
        return self.queries.search_instruments(query, limit)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        return self.queries.get_stats()
    
    def classify_instrument(self, isin: str) -> Optional[Dict[str, Any]]:
        """Get CFI-based classification for an instrument."""
        return self.queries.classify_instrument(isin)
    
    def get_instruments_by_cfi_category(self, category: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get instruments by CFI category."""
        return self.queries.get_instruments_by_cfi_category(category, limit)
    
    def get_latest_instruments(self, limit: Optional[int] = None):
        """Get latest instruments from the database."""
        return self.queries.get_latest_instruments(limit)
    
    def get_instruments_active_on_date(self, target_date: str, limit: Optional[int] = None):
        """Get instruments active on a specific date (history mode only)."""
        return self.queries.get_instruments_active_on_date(target_date, limit)
    
    def get_instrument_state_on_date(self, isin: str, target_date: str) -> Optional[Dict[str, Any]]:
        """Get instrument state as of a specific date (history mode only)."""
        return self.queries.get_instrument_state_on_date(isin, target_date)
    
    def get_instrument_version_history(self, isin: str):
        """Get complete version history for an instrument (history mode only)."""
        return self.queries.get_instrument_version_history(isin)
    
    def get_modified_instruments_since(self, since_date: str):
        """Get instruments modified since a specific date (history mode only)."""
        return self.queries.get_modified_instruments_since(since_date)
    
    def get_cancelled_instruments(self, since_date: Optional[str] = None):
        """Get cancelled instruments (history mode only)."""
        return self.queries.get_cancelled_instruments(since_date)
    
    # Versioning methods (history mode only)
    def process_delta_record(self, isin: str, record_type: str, record_data: Dict[str, Any],
                            publication_date: str, source_file: str) -> Dict[str, str]:
        """Process a delta file record (history mode only)."""
        if self.versioning is None:
            raise ValueError("Delta processing only available in history mode")
        return self.versioning.process_delta_record(isin, record_type, record_data, publication_date, source_file)
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get delta processing statistics (history mode only)."""
        if self.versioning is None:
            raise ValueError("Processing stats only available in history mode")
        return self.versioning.get_processing_stats()
    
    def validate_version_integrity(self) -> Dict[str, Any]:
        """Validate version integrity (history mode only)."""
        if self.versioning is None:
            raise ValueError("Version validation only available in history mode")
        return self.versioning.validate_version_integrity()


__all__ = ['DuckDBStorage', 'DuckDBConnection', 'DuckDBOperations', 'DuckDBQueries', 'DuckDBVersioning']