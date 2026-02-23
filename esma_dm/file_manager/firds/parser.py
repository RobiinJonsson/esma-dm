"""
FIRDS data parsing and indexing operations.
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd

from ..enums import AssetType
from esma_dm.utils.validators import validate_isin, validate_lei, validate_cfi


class FIRDSParser:
    """Handles data parsing, validation, and indexing operations."""
    
    def __init__(self, config, data_store):
        """Initialize parser with configuration and data store."""
        self.config = config
        self.data_store = data_store
        self.logger = logging.getLogger(__name__)
    
    # Validation methods - delegated to utils.validators module
    # Kept as staticmethods for backward compatibility
    @staticmethod
    def validate_isin(isin: str) -> bool:
        """
        Validate ISIN format (ISO 6166).
        
        Deprecated: Use esma_dm.utils.validators.validate_isin() directly.
        This method is kept for backward compatibility.
        
        Args:
            isin: ISIN code to validate
        
        Returns:
            True if valid ISIN format
        
        Example:
            >>> FIRDSParser.validate_isin('US0378331005')
            True
        """
        return validate_isin(isin)
    
    @staticmethod
    def validate_lei(lei: str) -> bool:
        """
        Validate LEI format (ISO 17442).
        
        Deprecated: Use esma_dm.utils.validators.validate_lei() directly.
        This method is kept for backward compatibility.
        
        Args:
            lei: LEI code to validate
        
        Returns:
            True if valid LEI format
        
        Example:
            >>> FIRDSParser.validate_lei('549300VALTPVHYSYMH70')
            True
        """
        return validate_lei(lei)
    
    @staticmethod
    def validate_cfi(cfi: str) -> bool:
        """
        Validate CFI code format (ISO 10962).
        
        Deprecated: Use esma_dm.utils.validators.validate_cfi() directly.
        This method is kept for backward compatibility.
        
        Args:
            cfi: CFI code to validate
        
        Returns:
            True if valid CFI format
        
        Example:
            >>> FIRDSParser.validate_cfi('ESVUFR')
            True
        """
        return validate_cfi(cfi)
    
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
            >>> parser = FIRDSParser(config, data_store)
            >>> ref = parser.reference('US0378331005')
            >>> 
            >>> if ref is not None:
            ...     print(f"Name: {ref.get('full_name')}")
            ...     print(f"CFI: {ref.get('cfi_code')}")
            ...     print(f"Currency: {ref.get('currency')}")
        """
        # Validate ISIN format
        if not validate_isin(isin):
            raise ValueError(f"Invalid ISIN format: {isin}")
        
        # Get from data store
        instrument_data = self.data_store.get_instrument(isin)
        
        if instrument_data:
            # Convert dict to Series
            return pd.Series(instrument_data)
        
        self.logger.warning(f"No reference data found for ISIN: {isin}")
        self.logger.info("Try downloading files first or use index_cached_files()")
        return None
    
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
            >>> parser = FIRDSParser(config, data_store)
            >>> # Index all latest FULINS files
            >>> stats = parser.index_cached_files()
            >>> 
            >>> # Index only equities
            >>> stats = parser.index_cached_files(asset_type='E')
            >>> 
            >>> # Index all equity files (not just latest)
            >>> stats = parser.index_cached_files(asset_type='E', latest_only=False)
            >>> 
            >>> # Index and delete CSV files
            >>> stats = parser.index_cached_files(delete_csv=True)
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
        
        # If latest_only, filter to most recent DATE per asset type (all files from that date)
        files_to_process = csv_files
        files_skipped = 0
        
        if latest_only:
            # Group files by asset type and date
            files_by_type_date = defaultdict(lambda: defaultdict(list))
            for f in csv_files:
                # Extract asset type and date from filename (e.g., FULINS_E_20260103_01of02_data.csv)
                parts = f.name.split('_')
                if len(parts) >= 3:
                    atype = parts[1]  # Second part is asset type
                    date = parts[2]   # Third part is date
                    files_by_type_date[atype][date].append(f)
            
            # Keep all files from the latest date per asset type
            files_to_process = []
            for atype, dates_dict in files_by_type_date.items():
                # Get the latest date for this asset type
                latest_date = max(dates_dict.keys())
                latest_files = dates_dict[latest_date]
                files_to_process.extend(latest_files)
                
                # Count skipped files from older dates
                total_files = sum(len(files) for files in dates_dict.values())
                skipped = total_files - len(latest_files)
                files_skipped += skipped
                
                if len(dates_dict) > 1:
                    self.logger.info(f"Asset type {atype}: using {len(latest_files)} files from {latest_date} (skipped {skipped} files from older dates)")
            
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
            >>> parser = FIRDSParser(config, data_store)
            >>> stats = parser.index_unloaded_fulins()
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
            self.logger.info(f"Found {len(indexed_files)} already indexed files")
        except Exception as e:
            self.logger.warning(f"Could not check indexed files: {e}")
        
        # Filter to unloaded files
        unloaded_files = []
        for f in fulins_files:
            if f.name not in indexed_files:
                unloaded_files.append(f)
            else:
                self.logger.debug(f"File already indexed: {f.name}")
        
        self.logger.info(f"Found {len(unloaded_files)} unloaded FULINS files")
        
        if not unloaded_files:
            return {
                'files_found': len(fulins_files),
                'files_already_indexed': len(fulins_files),
                'files_indexed': 0,
                'total_instruments': 0,
                'failed_files': []
            }
        
        # Index unloaded files
        total_instruments = 0
        failed_files = []
        
        for i, csv_file in enumerate(unloaded_files, 1):
            try:
                self.logger.info(f"[{i}/{len(unloaded_files)}] Indexing {csv_file.name}")
                count = self.data_store.index_csv_file(csv_file)
                total_instruments += count
                self.logger.info(f"  Indexed {count:,} instruments")
                
            except Exception as e:
                self.logger.error(f"  Failed to index {csv_file.name}: {e}")
                failed_files.append(csv_file.name)
        
        self.logger.info(f"Indexing complete: {len(unloaded_files) - len(failed_files)} files indexed, {total_instruments:,} instruments added")
        
        return {
            'files_found': len(fulins_files),
            'files_already_indexed': len(fulins_files) - len(unloaded_files),
            'files_indexed': len(unloaded_files) - len(failed_files),
            'total_instruments': total_instruments,
            'failed_files': failed_files
        }