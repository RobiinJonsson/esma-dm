"""
DuckDB bulk operations module.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

from .connection import DuckDBConnection
from ..bulk import BulkInserter
from ...models.utils import CFI


class DuckDBOperations:
    """Handles bulk insert, update, and data processing operations."""
    
    def __init__(self, connection: DuckDBConnection):
        """Initialize operations with database connection."""
        self.connection = connection
        self.logger = logging.getLogger(__name__)
    
    @property
    def con(self):
        """Get database connection."""
        self.connection._ensure_connection()
        return self.connection.con
    
    def update(self, asset_type: Optional[str] = None):
        """
        Update database from cached CSV files.
        
        Args:
            asset_type: Filter by asset type (E, D, O, etc.). If None, processes all.
        
        Returns:
            Dict with update statistics
        """
        start_time = time.time()
        self.connection._ensure_connection()
        
        # Find CSV files in cache directory
        csv_dir = self.connection.cache_dir
        
        if asset_type:
            # Filter for specific asset type files
            pattern = f"*{asset_type}_*.csv"
            self.logger.info(f"Updating from {asset_type} asset type files")
        else:
            pattern = "*.csv"
            self.logger.info("Updating from all CSV files")
        
        csv_files = list(csv_dir.glob(pattern))
        
        if not csv_files:
            self.logger.warning(f"No CSV files found in {csv_dir} matching pattern {pattern}")
            return {
                "status": "no_files",
                "files_processed": 0,
                "total_instruments": 0,
                "duration_seconds": 0
            }
        
        total_instruments = 0
        files_processed = 0
        
        for csv_file in sorted(csv_files):
            try:
                instruments_count = self.index_csv_file(csv_file)
                total_instruments += instruments_count
                files_processed += 1
                self.logger.info(f"Processed {csv_file.name}: {instruments_count} instruments")
            except Exception as e:
                self.logger.error(f"Failed to process {csv_file}: {e}")
                continue
        
        duration = time.time() - start_time
        self.logger.info(f"Update completed: {files_processed} files, {total_instruments} instruments in {duration:.2f}s")
        
        return {
            "status": "completed",
            "files_processed": files_processed,
            "total_instruments": total_instruments,
            "duration_seconds": duration
        }
    
    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """Find first matching column from a list of patterns."""
        for pattern in patterns:
            if pattern in df.columns:
                return pattern
        return None
    
    def _prepare_master_records(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Prepare master instrument records for bulk insert."""
        # Map various column name patterns
        isin_col = self._find_column(df, ['Isin', 'ISIN', 'isin'])
        cfi_col = self._find_column(df, ['CfiCd', 'CFI', 'cfi_code', 'CfiCode'])
        name_col = self._find_column(df, ['FullNm', 'FullName', 'full_name', 'Name'])
        issuer_col = self._find_column(df, ['Issr', 'Issuer', 'issuer'])
        
        if not isin_col:
            raise ValueError("ISIN column not found")
        
        # Create master records
        master_df = pd.DataFrame()
        master_df['isin'] = df[isin_col]
        master_df['cfi_code'] = df[cfi_col] if cfi_col else None
        master_df['full_name'] = df[name_col] if name_col else None
        master_df['issuer'] = df[issuer_col] if issuer_col else None
        master_df['source_file'] = source_file
        master_df['indexed_at'] = pd.Timestamp.now()
        
        # Add asset type based on CFI
        if cfi_col:
            master_df['asset_type'] = df[cfi_col].str[0] if cfi_col else None
        
        return master_df.dropna(subset=['isin'])
    
    def _insert_listings(self, df: pd.DataFrame, source_file: str):
        """Insert market listings if trading venue columns are present."""
        # Map listing columns
        isin_col = self._find_column(df, ['Isin', 'ISIN', 'isin'])
        venue_col = self._find_column(df, ['TradgVnIssr', 'TradingVenue', 'trading_venue'])
        segment_col = self._find_column(df, ['Sgmt', 'Segment', 'segment'])
        
        if not all([isin_col, venue_col]):
            self.logger.debug("Missing required columns for listings")
            return
        
        # Prepare listings data
        listings_df = pd.DataFrame()
        listings_df['isin'] = df[isin_col]
        listings_df['trading_venue'] = df[venue_col]
        listings_df['segment'] = df[segment_col] if segment_col else None
        listings_df['source_file'] = source_file
        listings_df['indexed_at'] = pd.Timestamp.now()
        
        # Remove rows with null required values
        listings_df = listings_df.dropna(subset=['isin', 'trading_venue'])
        
        if len(listings_df) == 0:
            return
        
        # Insert into listings table
        try:
            self.con.execute("BEGIN TRANSACTION")
            
            # Use pandas to_sql for efficient bulk insert
            listings_df.to_sql('listings', self.con, if_exists='append', index=False, method='multi')
            
            self.con.execute("COMMIT")
            self.logger.debug(f"Inserted {len(listings_df)} market listings")
            
        except Exception as e:
            self.con.execute("ROLLBACK")
            self.logger.error(f"Failed to insert listings: {e}")
            raise
    
    def index_csv_file(self, csv_path: Path) -> int:
        """
        Index a single CSV file into the database.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Number of instruments processed
            
        Raises:
            Exception: If file processing fails
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        start_time = time.time()
        source_file = csv_path.name
        
        self.logger.info(f"Starting to index {source_file}")
        
        try:
            # Load CSV with error handling for different encodings
            try:
                df = pd.read_csv(csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                self.logger.warning(f"UTF-8 decode failed for {csv_path}, trying ISO-8859-1")
                df = pd.read_csv(csv_path, encoding='iso-8859-1')
            
            if len(df) == 0:
                self.logger.warning(f"Empty CSV file: {csv_path}")
                return 0
            
            self.logger.info(f"Loaded {len(df)} records from {source_file}")
            
            # Prepare master records
            master_df = self._prepare_master_records(df, source_file)
            
            if len(master_df) == 0:
                self.logger.warning(f"No valid instruments found in {source_file}")
                return 0
            
            # Group by asset type for bulk processing
            instruments_by_type = master_df.groupby('asset_type') if 'asset_type' in master_df.columns else {'unknown': master_df}
            
            self.con.execute("BEGIN TRANSACTION")
            
            total_inserted = 0
            
            try:
                # Process each asset type separately for optimal performance
                for asset_type, asset_df in instruments_by_type:
                    if pd.isna(asset_type):
                        asset_type = 'unknown'
                    
                    self.logger.debug(f"Processing {len(asset_df)} {asset_type} instruments")
                    
                    # Use BulkInserter for efficient batch processing
                    inserter = BulkInserter(self.con, mode=self.connection.mode)
                    
                    # Prepare vectors for bulk insert
                    vectors = inserter.prepare_vectors(asset_df, asset_type)
                    
                    # Bulk insert
                    inserted_count = inserter.insert_batch(vectors, asset_type)
                    total_inserted += inserted_count
                    
                    self.logger.debug(f"Inserted {inserted_count} {asset_type} instruments")
                
                # Insert market listings if data is available
                self._insert_listings(df, source_file)
                
                self.con.execute("COMMIT")
                
                duration = time.time() - start_time
                rate = total_inserted / duration if duration > 0 else 0
                
                self.logger.info(
                    f"Successfully indexed {source_file}: {total_inserted} instruments "
                    f"in {duration:.2f}s ({rate:.0f} instruments/sec)"
                )
                
                return total_inserted
                
            except Exception as e:
                self.con.execute("ROLLBACK")
                self.logger.error(f"Transaction failed for {source_file}: {e}")
                raise
            
        except Exception as e:
            self.logger.error(f"Failed to index {csv_path}: {e}")
            raise
    
    def index_all_csv_files(self, csv_dir: Path, pattern: str = "*.csv", delete_csv: bool = False) -> dict:
        """
        Index all CSV files in a directory.
        
        Args:
            csv_dir: Directory containing CSV files
            pattern: File pattern to match (default: "*.csv")
            delete_csv: Whether to delete CSV files after processing
            
        Returns:
            Dict with processing statistics
        """
        if not csv_dir.exists():
            raise FileNotFoundError(f"Directory not found: {csv_dir}")
        
        csv_files = list(csv_dir.glob(pattern))
        
        if not csv_files:
            self.logger.warning(f"No files matching pattern '{pattern}' found in {csv_dir}")
            return {"files_processed": 0, "total_instruments": 0}
        
        start_time = time.time()
        total_instruments = 0
        files_processed = 0
        failed_files = []
        
        for csv_file in sorted(csv_files):
            try:
                instruments_count = self.index_csv_file(csv_file)
                total_instruments += instruments_count
                files_processed += 1
                
                # Delete file if requested and processing succeeded
                if delete_csv:
                    csv_file.unlink()
                    self.logger.debug(f"Deleted processed file: {csv_file}")
                    
            except Exception as e:
                failed_files.append({"file": csv_file.name, "error": str(e)})
                self.logger.error(f"Failed to process {csv_file}: {e}")
        
        duration = time.time() - start_time
        
        result = {
            "files_processed": files_processed,
            "total_instruments": total_instruments,
            "failed_files": failed_files,
            "duration_seconds": duration
        }
        
        self.logger.info(
            f"Batch processing completed: {files_processed} files, "
            f"{total_instruments} instruments in {duration:.2f}s"
        )
        
        return result