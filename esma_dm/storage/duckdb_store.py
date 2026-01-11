"""
DuckDB storage backend with vectorized bulk loading.

Architecture:
- Master table (instruments): Core fields for all instruments
- Asset-specific tables: 10 tables for each asset type (E, D, F, O, H, S, J, R, C, I)
- Vectorized processing: Group by asset type, bulk insert
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import duckdb
import pandas as pd
import numpy as np

from .base import StorageBackend
from .schema import initialize_schema
from .bulk_inserters import BulkInserter
from ..models.utils import CFI
from .. import config as global_config


class DuckDBStorage(StorageBackend):
    """
    DuckDB storage with star schema and vectorized bulk loading.
    
    Schema:
    - instruments (master table): ISIN, CFI, type, issuer, name
    - 10 asset-specific tables: equity, debt, futures, options, swap, forward, rights, civ, spot
    
    Performance: Bulk insert entire CSV in one transaction per asset type.
    """
    
    def __init__(self, cache_dir: Path, db_path: Optional[str] = None):
        """Initialize DuckDB storage."""
        super().__init__(cache_dir)
        self.logger = logging.getLogger(__name__)
        
        if db_path is None:
            self.db_path = str(self.cache_dir / 'firds.db')
        else:
            self.db_path = db_path
        
        self.con = None
        self._ensure_connection()
    
    def _ensure_connection(self):
        """Ensure database connection is established."""
        if self.con is None:
            self.con = duckdb.connect(self.db_path)
    
    def initialize(self, mode: str = 'current', verify_only: bool = False):
        """
        Initialize database and verify structure matches data models.
        
        This method should be called after package installation and before loading data.
        If a database already exists, it verifies the schema structure but does not
        reinitialize unless explicitly dropped first.
        
        Workflow:
            1. Install: pip install -e .
            2. Initialize: firds.data_store.initialize()
            3. Load: firds.get_latest_full_files() and firds.index_cached_files()
        
        Args:
            mode: 'current' for FULINS-based snapshots (default) or 'delta' for incremental updates
            verify_only: If True, only verify existing schema without creating tables
        
        Returns:
            Dict with initialization status and verification results
        
        Raises:
            ValueError: If mode is invalid or schema verification fails
        
        Example:
            >>> from esma_dm import FIRDSClient
            >>> firds = FIRDSClient()
            >>> 
            >>> # First time setup
            >>> result = firds.data_store.initialize(mode='current')
            >>> print(f"Status: {result['status']}")
            >>> print(f"Tables: {result['tables_verified']}")
            >>> 
            >>> # Verify existing database
            >>> result = firds.data_store.initialize(verify_only=True)
        """
        self._ensure_connection()
        
        if mode not in ('current', 'delta'):
            raise ValueError(f"Invalid mode: {mode}. Use 'current' or 'delta'")
        
        if mode == 'delta':
            raise NotImplementedError("Delta mode not yet implemented")
        
        # Check if database already exists
        db_file = Path(self.db_path)
        db_exists = db_file.exists() and db_file.stat().st_size > 0
        
        # Get expected tables from schema
        expected_tables = {
            'instruments', 'listings', 'equity_instruments', 'debt_instruments',
            'futures_instruments', 'option_instruments', 'swap_instruments',
            'forward_instruments', 'rights_instruments', 'civ_instruments',
            'spot_instruments', 'metadata'
        }
        
        # Check existing tables
        existing_tables = set()
        try:
            tables_df = self.con.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchdf()
            existing_tables = set(tables_df['table_name'].tolist())
        except Exception as e:
            self.logger.debug(f"Could not query existing tables: {e}")
        
        if verify_only:
            if not db_exists:
                raise ValueError("Cannot verify schema: database does not exist")
            
            missing_tables = expected_tables - existing_tables
            if missing_tables:
                raise ValueError(f"Schema verification failed: missing tables {missing_tables}")
            
            self.logger.info(f"Schema verification passed: {len(existing_tables)} tables verified")
            return {
                'status': 'verified',
                'mode': mode,
                'tables_verified': len(existing_tables),
                'existing_tables': sorted(existing_tables)
            }
        
        # Initialize schema if needed
        if db_exists and existing_tables:
            self.logger.info(f"Database already exists with {len(existing_tables)} tables")
            missing_tables = expected_tables - existing_tables
            
            if missing_tables:
                self.logger.warning(f"Missing tables detected: {missing_tables}")
                self.logger.info("Creating missing tables...")
                initialize_schema(self.con)
            else:
                self.logger.info("Schema verification passed - all tables present")
        else:
            self.logger.info(f"Initializing new database: {self.db_path}")
            initialize_schema(self.con)
        
        # Verify schema structure
        verification_results = self._verify_schema_structure()
        
        if not verification_results['all_verified']:
            raise ValueError(f"Schema verification failed: {verification_results['errors']}")
        
        self.logger.info(f"Database initialized successfully in {mode} mode")
        
        return {
            'status': 'initialized',
            'mode': mode,
            'database_path': self.db_path,
            'tables_created': len(expected_tables),
            'tables_verified': len(verification_results['verified_tables']),
            'verification': verification_results
        }
    
    def _verify_schema_structure(self) -> Dict[str, Any]:
        """
        Verify that database schema matches expected structure from data models.
        
        Returns:
            Dictionary with verification results including status and any errors
        """
        errors = []
        verified_tables = []
        
        # Define expected columns for key tables
        expected_columns = {
            'instruments': {'isin', 'cfi_code', 'instrument_type', 'issuer', 'full_name', 'currency'},
            'listings': {'id', 'isin', 'trading_venue_id', 'first_trade_date', 'termination_date'},
            'equity_instruments': {'isin', 'short_name', 'voting_rights_per_share'},
            'debt_instruments': {'isin', 'short_name', 'maturity_date', 'interest_rate_type'},
            'futures_instruments': {'isin', 'short_name', 'expiry_date', 'delivery_type'},
            'option_instruments': {'isin', 'short_name', 'option_type', 'strike_price'},
            'swap_instruments': {'isin', 'short_name', 'expiry_date'},
            'forward_instruments': {'isin', 'short_name', 'expiry_date'},
            'rights_instruments': {'isin', 'short_name', 'expiry_date'},
            'civ_instruments': {'isin', 'short_name'},
            'spot_instruments': {'isin', 'short_name'},
            'metadata': {'file_name', 'indexed_at', 'instruments_count'}
        }
        
        for table_name, required_cols in expected_columns.items():
            try:
                # Get actual columns
                columns_df = self.con.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """).fetchdf()
                
                actual_cols = set(columns_df['column_name'].tolist())
                missing_cols = required_cols - actual_cols
                
                if missing_cols:
                    errors.append(f"Table {table_name} missing columns: {missing_cols}")
                else:
                    verified_tables.append(table_name)
                    
            except Exception as e:
                errors.append(f"Could not verify table {table_name}: {e}")
        
        return {
            'all_verified': len(errors) == 0,
            'verified_tables': verified_tables,
            'errors': errors,
            'total_tables_checked': len(expected_columns)
        }
    
    def drop(self, confirm: bool = False):
        """
        Drop database by closing connection and deleting file.
        
        This completely removes the database file from disk, clearing all data.
        Use this to start fresh or ensure no data is stored in memory/disk.
        
        Args:
            confirm: Safety check - must be True to actually drop database
        
        Returns:
            Dict with drop operation status
        
        Raises:
            ValueError: If confirm is not True
        
        Example:
            >>> from esma_dm import FIRDSClient
            >>> firds = FIRDSClient()
            >>> 
            >>> # Drop database (requires confirmation)
            >>> result = firds.data_store.drop(confirm=True)
            >>> print(f"Status: {result['status']}")
            >>> 
            >>> # Reinitialize fresh database
            >>> firds.data_store.initialize(mode='current')
        """
        if not confirm:
            raise ValueError(
                "Database drop requires explicit confirmation. "
                "Call drop(confirm=True) to proceed. "
                "Warning: This will permanently delete all data."
            )
        
        db_file = Path(self.db_path)
        db_existed = db_file.exists()
        file_size = 0
        
        if db_existed:
            file_size = db_file.stat().st_size
        
        # Close connection first
        if self.con:
            try:
                self.con.close()
                self.logger.info("Database connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
            finally:
                self.con = None
        
        # Delete database file
        if db_file.exists():
            try:
                db_file.unlink()
                self.logger.info(f"Deleted database: {self.db_path} ({file_size:,} bytes)")
                return {
                    'status': 'dropped',
                    'database_path': self.db_path,
                    'file_size_bytes': file_size,
                    'existed': True
                }
            except Exception as e:
                self.logger.error(f"Failed to delete database file: {e}")
                raise
        else:
            self.logger.warning(f"Database file not found: {self.db_path}")
            return {
                'status': 'not_found',
                'database_path': self.db_path,
                'file_size_bytes': 0,
                'existed': False
            }
    
    def update(self, asset_type: Optional[str] = None):
        """
        Update database with newer FULINS files (true snapshot replacement).
        
        Args:
            asset_type: Single asset type to update (e.g., 'E', 'D'). 
                       If None, drops all tables and rebuilds entire database.
        
        Returns:
            Dict with update summary (files_updated, instruments_added, etc.)
        
        Example:
            >>> db = DuckDBStorage(cache_dir)
            >>> result = db.update(asset_type='E')  # Update just equities
            >>> result = db.update()  # Update all types (full rebuild)
        """
        self._ensure_connection()
        
        # Import here to avoid circular dependency
        from ..firds import FIRDSClient
        
        # Create FIRDS client and download latest files
        firds = FIRDSClient()
        
        # If no asset type specified, do full rebuild
        if asset_type is None:
            self.logger.info("No asset type specified - performing full database rebuild")
            
            # Download all latest FULINS files
            for atype in global_config.ASSET_TYPES_ALL:
                try:
                    firds.get_latest_full_files(asset_type=atype)
                    self.logger.info(f"Downloaded latest {atype} files")
                except Exception as e:
                    self.logger.warning(f"Could not download {atype} files: {e}")
            
            # Drop and rebuild
            self.drop()
            self.initialize()
            
            # Index all files
            cache_dir = Path(firds.config.downloads_path) / 'firds'
            results = self.index_all_csv_files(cache_dir, delete_csv=False)
            
            return {
                'files_updated': results['files_processed'],
                'instruments_added': results['total_instruments'],
                'listings_added': results.get('total_listings', 0),
                'instruments_removed': 0,
                'listings_removed': 0,
                'asset_types_updated': ['ALL']
            }
        
        # Single asset type update
        self.logger.info(f"Updating asset type: {asset_type}")
        
        # Download latest file for this asset type
        firds.get_latest_full_files(asset_type=asset_type)
        
        # Get latest CSV file from cache
        cache_dir = Path(firds.config.downloads_path) / 'firds'
        csv_files = list(cache_dir.glob(f'FULINS_{asset_type}_*.csv'))
        
        if not csv_files:
            raise FileNotFoundError(f"No cached CSV files found for asset type {asset_type}")
        
        # Map asset type to table name
        table_name = global_config.ASSET_TABLE_MAP.get(asset_type)
        
        if not table_name:
            raise ValueError(f"Unknown asset type: {asset_type}")
        
        # Get ISINs in this asset table before dropping
        try:
            old_isins = self.con.execute(f"SELECT DISTINCT isin FROM {table_name}").fetchdf()['isin'].tolist()
            self.logger.info(f"Found {len(old_isins)} ISINs in {table_name} table")
        except Exception:
            old_isins = []
            self.logger.info(f"Table {table_name} does not exist or is empty")
        
        # STEP 1: Delete all listings for these ISINs
        listings_deleted = 0
        if old_isins:
            placeholders = ','.join(['?'] * len(old_isins))
            listings_deleted = self.con.execute(
                f"DELETE FROM listings WHERE isin IN ({placeholders})",
                old_isins
            ).fetchone()[0]
            self.logger.info(f"Deleted {listings_deleted} listings")
        
        # STEP 2: Drop the asset-specific table
        self.con.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.logger.info(f"Dropped table: {table_name}")
        
        # STEP 3: Find orphaned instruments (not in any other asset table)
        instruments_deleted = 0
        if old_isins:
            other_tables = [t for t in table_map.values() if t != table_name]
            
            # Build UNION query to find ISINs in other tables
            isin_checks = []
            for other_table in other_tables:
                try:
                    table_exists = self.con.execute(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                        [other_table]
                    ).fetchone()[0] > 0
                    
                    if table_exists:
                        isin_checks.append(f"SELECT isin FROM {other_table}")
                except Exception:
                    continue
            
            if isin_checks:
                union_query = " UNION ".join(isin_checks)
                orphaned_isins = self.con.execute(f"""
                    SELECT isin FROM (VALUES {','.join(['(?)'] * len(old_isins))}) AS t(isin)
                    WHERE isin NOT IN ({union_query})
                """, old_isins).fetchdf()['isin'].tolist()
            else:
                orphaned_isins = old_isins
            
            if orphaned_isins:
                placeholders = ','.join(['?'] * len(orphaned_isins))
                instruments_deleted = self.con.execute(
                    f"DELETE FROM instruments WHERE isin IN ({placeholders})",
                    orphaned_isins
                ).fetchone()[0]
                self.logger.info(f"Deleted {instruments_deleted} orphaned instruments")
        
        # STEP 4: Delete old metadata entries for this asset type
        self.con.execute(
            "DELETE FROM metadata WHERE file_name LIKE ? AND file_type = 'FULINS'",
            [f'%{asset_type}%']
        )
        
        # STEP 5: Re-index the CSV files
        results = self.index_all_csv_files(cache_dir, delete_csv=False)
        
        return {
            'files_updated': results['files_processed'],
            'instruments_added': results['total_instruments'],
            'listings_added': results.get('total_listings', 0),
            'instruments_removed': instruments_deleted,
            'listings_removed': listings_deleted,
            'asset_types_updated': [asset_type]
        }
    
    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """Find column in DataFrame matching any of the patterns."""
        for pattern in patterns:
            matches = [col for col in df.columns if pattern in col]
            if matches:
                return matches[0]
        return None
    
    def _prepare_master_records(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Extract master table fields from CSV."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        full_name_col = self._find_column(df, ['FullNm', 'full_name'])
        cfi_col = self._find_column(df, ['ClssfctnTp', 'CfiCd', 'classification_type', 'cfi_code'])
        issuer_col = self._find_column(df, ['Issr', 'issuer'])
        currency_col = self._find_column(df, ['NtnlCcy', 'notional_currency', 'currency'])
        
        cfi_code = df[cfi_col] if cfi_col else None
        instrument_type = cfi_code.str[0] if cfi_code is not None else None
        
        master_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'cfi_code': cfi_code,
            'instrument_type': instrument_type,
            'issuer': df[issuer_col] if issuer_col else None,
            'full_name': df[full_name_col] if full_name_col else None,
            'currency': df[currency_col] if currency_col else None,
            'source_file': source_file,
            'indexed_at': pd.Timestamp.now(),
            'updated_at': pd.Timestamp.now()
        })
        
        master_df = master_df.dropna(subset=['isin'])
        return master_df
    
    def _insert_listings(self, df: pd.DataFrame, source_file: str):
        """Insert trading venue listings (one row per ISIN-venue combination)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        venue_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_Id', 'TradgVnRltdAttrbts_Id', 'TradgVnId'])
        first_trade_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_FrstTradDt', 'TradgVnRltdAttrbts_FrstTradDt', 'FrstTradDt'])
        term_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_TermntnDt', 'TradgVnRltdAttrbts_TermntnDt', 'TermntnDt'])
        admission_approval_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_AdmssnApprvlDtByIssr', 'TradgVnRltdAttrbts_AdmssnApprvlDtByIssr'])
        request_admission_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_ReqForAdmssnDt', 'TradgVnRltdAttrbts_ReqForAdmssnDt'])
        issuer_req_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_IssrReq', 'TradgVnRltdAttrbts_IssrReq'])
        
        if not isin_col:
            return
        
        listings_df = pd.DataFrame({
            'isin': df[isin_col],
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None,
            'admission_approval_date': pd.to_datetime(df[admission_approval_col], errors='coerce') if admission_approval_col else None,
            'request_for_admission_date': pd.to_datetime(df[request_admission_col], errors='coerce') if request_admission_col else None,
            'issuer_request': df[issuer_req_col] if issuer_req_col else None,
            'source_file': source_file,
            'indexed_at': pd.Timestamp.now()
        })
        
        listings_df = listings_df.dropna(subset=['isin'])
        
        if len(listings_df) > 0:
            self.con.execute("""
                INSERT INTO listings (isin, trading_venue_id, first_trade_date, termination_date,
                                     admission_approval_date, request_for_admission_date, issuer_request,
                                     source_file, indexed_at)
                SELECT isin, trading_venue_id, first_trade_date, termination_date,
                       admission_approval_date, request_for_admission_date, issuer_request,
                       source_file, indexed_at
                FROM listings_df
                WHERE trading_venue_id IS NOT NULL
            """)
    
    def index_csv_file(self, csv_path: Path) -> int:
        """
        Index a FIRDS CSV file.
        
        Strategy:
        1. Read entire CSV into DataFrame
        2. Prepare master records
        3. Detect asset type (CFI first character)
        4. Bulk insert master records
        5. Route to asset-specific bulk insert handler
        """
        start_time = time.time()
        csv_path = Path(csv_path) if isinstance(csv_path, str) else csv_path
        source_file = csv_path.name
        self.logger.info(f"Indexing {source_file}")
        
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            self.logger.info(f"Loaded {len(df):,} records")
            
            master_df = self._prepare_master_records(df, source_file)
            
            if len(master_df) == 0:
                self.logger.warning("No valid records found")
                return 0
            
            instrument_type = master_df['instrument_type'].iloc[0] if len(master_df) > 0 else None
            self.logger.info(f"Detected instrument type: {instrument_type} (sample CFI: {master_df['cfi_code'].iloc[0] if len(master_df) > 0 else 'N/A'})")
            
            self.con.execute("""
                INSERT INTO instruments 
                SELECT * FROM master_df
                ON CONFLICT (isin) DO UPDATE SET
                    cfi_code = EXCLUDED.cfi_code,
                    instrument_type = EXCLUDED.instrument_type,
                    issuer = EXCLUDED.issuer,
                    full_name = EXCLUDED.full_name,
                    currency = EXCLUDED.currency,
                    source_file = EXCLUDED.source_file,
                    indexed_at = EXCLUDED.indexed_at,
                    updated_at = EXCLUDED.updated_at
            """)
            
            # Insert listings (one row per ISIN-venue combination)
            self._insert_listings(df, source_file)
            
            inserter = BulkInserter(self.con, self._find_column)
            
            if instrument_type == 'E':
                inserter.insert_equities(df)
            elif instrument_type == 'D':
                inserter.insert_debt(df)
            elif instrument_type == 'F':
                inserter.insert_futures(df)
            elif instrument_type == 'O':
                inserter.insert_options(df)
            elif instrument_type == 'H':
                inserter.insert_options(df)
            elif instrument_type == 'S':
                inserter.insert_swaps(df)
            elif instrument_type == 'J':
                inserter.insert_forwards(df)
            elif instrument_type == 'R':
                inserter.insert_rights(df)
            elif instrument_type == 'C':
                inserter.insert_civs(df)
            elif instrument_type == 'I':
                inserter.insert_spots(df)
            else:
                self.logger.warning(f"Unknown instrument type: {instrument_type} for {source_file}")
            
            self.con.execute("""
                INSERT INTO metadata (file_name, instruments_count, indexed_at)
                VALUES (?, ?, ?)
                ON CONFLICT (file_name) DO UPDATE SET
                    instruments_count = EXCLUDED.instruments_count,
                    indexed_at = EXCLUDED.indexed_at
            """, [source_file, len(master_df), datetime.now()])
            
            elapsed = time.time() - start_time
            rate = len(master_df) / elapsed if elapsed > 0 else 0
            self.logger.info(f"Indexed {len(master_df):,} instruments in {elapsed:.1f}s ({rate:.0f} inst/sec)")
            
            return len(master_df)
            
        except Exception as e:
            self.logger.error(f"Error indexing {source_file}: {e}", exc_info=True)
            raise
    
    def get_instrument(self, isin: str) -> Optional[Dict[str, Any]]:
        """Retrieve instrument by ISIN with type-specific fields and CFI classification."""
        result = self.con.execute("""
            SELECT * FROM instruments WHERE isin = ?
        """, [isin]).fetchone()
        
        if not result:
            return None
        
        columns = [desc[0] for desc in self.con.description]
        instrument = dict(zip(columns, result))
        
        # Add CFI classification if CFI code exists
        cfi_code = instrument.get('cfi_code')
        if cfi_code:
            try:
                cfi = CFI(cfi_code)
                instrument['cfi_classification'] = cfi.describe()
            except Exception:
                pass  # Skip if CFI code is invalid
        
        instrument_type = instrument.get('instrument_type')
        
        if instrument_type == 'E':
            detail_result = self.con.execute("""
                SELECT * FROM equity_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'D':
            detail_result = self.con.execute("""
                SELECT * FROM debt_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'F':
            detail_result = self.con.execute("""
                SELECT * FROM futures_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type in ('O', 'H'):
            detail_result = self.con.execute("""
                SELECT * FROM option_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'S':
            detail_result = self.con.execute("""
                SELECT * FROM swap_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'J':
            detail_result = self.con.execute("""
                SELECT * FROM forward_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'R':
            detail_result = self.con.execute("""
                SELECT * FROM rights_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'C':
            detail_result = self.con.execute("""
                SELECT * FROM civ_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        elif instrument_type == 'I':
            detail_result = self.con.execute("""
                SELECT * FROM spot_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if detail_result:
                detail_columns = [desc[0] for desc in self.con.description]
                instrument.update(dict(zip(detail_columns, detail_result)))
        
        return instrument
    
    def get_stats_by_asset_type(self) -> Dict[str, int]:
        """Get count of instruments by asset type."""
        result = self.con.execute("""
            SELECT instrument_type, COUNT(*) as count
            FROM instruments
            GROUP BY instrument_type
            ORDER BY count DESC
        """).fetchall()
        
        return {row[0]: row[1] for row in result}
    
    def get_instrument_history(self, isin: str) -> List[Dict[str, Any]]:
        """Get all historical records for an instrument."""
        results = self.con.execute("""
            SELECT source_file, indexed_at
            FROM instruments
            WHERE isin = ?
            ORDER BY indexed_at DESC
        """, [isin]).fetchall()
        
        return [{'source_file': r[0], 'indexed_at': r[1]} for r in results]
    
    def index_all_csv_files(self, csv_dir: Path, pattern: str = "*.csv", delete_csv: bool = False) -> dict:
        """Index all CSV files in directory, skipping DLTINS files."""
        total = 0
        files_processed = 0
        failed_files = []
        
        for csv_file in sorted(csv_dir.glob(pattern)):
            # Skip DLTINS files - only process FULINS (full snapshots)
            if 'DLTINS' in csv_file.name or 'inspection' in csv_file.name or 'columns' in csv_file.name:
                continue
            
            try:
                count = self.index_csv_file(csv_file)
                total += count
                files_processed += 1
                
                # Delete CSV file after successful processing if requested
                if delete_csv:
                    csv_file.unlink()
                self.logger.info(f"Indexed {csv_file.name}: {count:,} instruments")
            except Exception as e:
                self.logger.error(f"Failed to index {csv_file.name}: {e}")
                failed_files.append(csv_file.name)
        
        return {
            'total_instruments': total,
            'files_processed': files_processed,
            'failed_files': failed_files
        }
    
    def search_instruments(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search instruments by name or ISIN with CFI classification."""
        results = self.con.execute("""
            SELECT isin, full_name, instrument_type, issuer, currency, cfi_code
            FROM instruments
            WHERE isin LIKE ? OR full_name LIKE ?
            LIMIT ?
        """, [f"%{query}%", f"%{query}%", limit]).fetchall()
        
        columns = ['isin', 'full_name', 'instrument_type', 'issuer', 'currency', 'cfi_code']
        instruments = [dict(zip(columns, row)) for row in results]
        
        # Add CFI classification to each result
        for inst in instruments:
            if inst.get('cfi_code'):
                try:
                    cfi = CFI(inst['cfi_code'])
                    inst['category_description'] = cfi.category_description
                    inst['group_description'] = cfi.group_description
                except Exception:
                    pass
        
        return instruments
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        total = self.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        by_type = self.get_stats_by_asset_type()
        
        files = self.con.execute("""
            SELECT COUNT(*), MAX(indexed_at)
            FROM metadata
        """).fetchone()
        
        return {
            'total_instruments': total,
            'by_type': by_type,
            'files_indexed': files[0] if files else 0,
            'last_indexed': files[1] if files else None
        }
    
    def classify_instrument(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Classify an instrument using its CFI code with full ISO 10962 decoding.
        
        Args:
            isin: Instrument ISIN
            
        Returns:
            Dictionary with full CFI classification or None if not found
        """
        result = self.con.execute("""
            SELECT isin, full_name, cfi_code, instrument_type, issuer, currency
            FROM instruments 
            WHERE isin = ?
        """, [isin]).fetchone()
        
        if not result:
            return None
        
        isin, name, cfi_code, inst_type, issuer, currency = result
        
        if not cfi_code:
            return {
                'isin': isin,
                'name': name,
                'instrument_type': inst_type,
                'issuer': issuer,
                'currency': currency,
                'cfi_code': None,
                'classification': 'CFI code not available'
            }
        
        try:
            cfi = CFI(cfi_code)
            classification = cfi.describe()
            
            return {
                'isin': isin,
                'name': name,
                'instrument_type': inst_type,
                'issuer': issuer,
                'currency': currency,
                'cfi_code': cfi_code,
                'classification': classification
            }
        except Exception as e:
            return {
                'isin': isin,
                'name': name,
                'instrument_type': inst_type,
                'issuer': issuer,
                'currency': currency,
                'cfi_code': cfi_code,
                'classification': f'Invalid CFI code: {e}'
            }
    
    def get_instruments_by_cfi_category(self, category: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get instruments by CFI category with full classification.
        
        Args:
            category: CFI category (E, D, C, F, O, S, H, R, I, J)
            limit: Maximum number of results
            
        Returns:
            List of instruments with classifications
        """
        results = self.con.execute("""
            SELECT isin, full_name, cfi_code, instrument_type, issuer, currency
            FROM instruments
            WHERE cfi_code LIKE ?
            LIMIT ?
        """, [f"{category}%", limit]).fetchall()
        
        instruments = []
        for row in results:
            isin, name, cfi_code, inst_type, issuer, currency = row
            
            try:
                cfi = CFI(cfi_code)
                instruments.append({
                    'isin': isin,
                    'name': name,
                    'cfi_code': cfi_code,
                    'instrument_type': inst_type,
                    'issuer': issuer,
                    'currency': currency,
                    'category': cfi.category_description,
                    'group': cfi.group_description
                })
            except Exception:
                instruments.append({
                    'isin': isin,
                    'name': name,
                    'cfi_code': cfi_code,
                    'instrument_type': inst_type,
                    'issuer': issuer,
                    'currency': currency,
                    'category': 'Invalid CFI',
                    'group': 'Invalid CFI'
                })
        
        return instruments
    
    def close(self):
        """Close database connection."""
        if hasattr(self, 'con'):
            self.con.close()
