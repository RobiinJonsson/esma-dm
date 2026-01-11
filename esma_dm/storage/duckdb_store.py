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
    
    def __init__(self, cache_dir: Path, db_path: Optional[str] = None, mode: str = 'current'):
        """Initialize DuckDB storage."""
        super().__init__(cache_dir)
        self.logger = logging.getLogger(__name__)
        self.mode = mode
        
        if db_path is None:
            # Use mode-specific database name
            db_name = f'firds_{mode}.duckdb'
            self.db_path = str(self.cache_dir / db_name)
        else:
            self.db_path = db_path
        
        self.con = None
        self._ensure_connection()
    
    def _ensure_connection(self):
        """Ensure database connection is established."""
        if self.con is None:
            self.con = duckdb.connect(self.db_path)
    
    def initialize(self, mode: Optional[str] = None, verify_only: bool = False):
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
            mode: 'current' for FULINS-based snapshots or 'history' for version tracking (defaults to instance mode)
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
        
        # Use instance mode if not specified
        if mode is None:
            mode = self.mode
        
        if mode not in ['current', 'history']:
            raise ValueError(f"Invalid mode '{mode}'. Must be 'current' or 'history'")
        
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
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt', 'TechAttrbts_PblctnPrd_FrDt', 'PblctnPrd_FrDt'])
        record_type_col = self._find_column(df, ['_record_type'])
        
        cfi_code = df[cfi_col] if cfi_col else None
        instrument_type = cfi_code.str[0] if cfi_code is not None else None
        
        # Mode-specific column handling:
        # - 'current' mode: minimal columns, no historical tracking
        # - 'history' mode: include historical tracking fields
        is_delta = 'DLTINS' in source_file
        
        base_df = pd.DataFrame({
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
        
        # Add historical tracking fields only in history mode
        if self.mode == 'history':
            publication_date = pd.to_datetime(df[publication_date_col], errors='coerce').iloc[0] if publication_date_col and len(df) > 0 else pd.Timestamp.now()
            base_df['valid_from_date'] = publication_date if not is_delta else None
            base_df['valid_to_date'] = None
            base_df['latest_record_flag'] = True if not is_delta else None
            base_df['record_type'] = df[record_type_col] if record_type_col else 'NEW'
            base_df['version_number'] = 1 if not is_delta else None
            base_df['source_file_type'] = 'DLTINS' if is_delta else 'FULINS'
            base_df['last_update_timestamp'] = pd.Timestamp.now()
            base_df['inconsistency_indicator'] = None
        
        master_df = base_df.dropna(subset=['isin'])
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
            
            # Build INSERT statement based on mode
            if self.mode == 'current':
                # Current mode: simple UPDATE without historical fields
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
            else:  # history mode
                # History mode: include all historical tracking fields
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
                        updated_at = EXCLUDED.updated_at,
                        valid_from_date = EXCLUDED.valid_from_date,
                        valid_to_date = EXCLUDED.valid_to_date,
                        latest_record_flag = EXCLUDED.latest_record_flag,
                        record_type = EXCLUDED.record_type,
                        version_number = EXCLUDED.version_number,
                        source_file_type = EXCLUDED.source_file_type,
                        last_update_timestamp = EXCLUDED.last_update_timestamp,
                        inconsistency_indicator = EXCLUDED.inconsistency_indicator
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
    
    def get_latest_instruments(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get all latest (current) versions of instruments.
        
        Per ESMA Section 9: Query pattern for latest versions.
        
        Args:
            limit: Optional limit on results
            
        Returns:
            DataFrame with current instrument versions
            
        Example:
            >>> storage.get_latest_instruments(limit=1000)
        """
        sql = """
            SELECT * FROM instruments
            WHERE latest_record_flag = TRUE
        """
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.con.execute(sql).fetchdf()
    
    def get_instruments_active_on_date(self, target_date: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get instruments that were active on a specific date.
        
        Per ESMA Section 9: Query instruments active on date T using Field11/Field12.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            limit: Optional limit on results
            
        Returns:
            DataFrame with instruments active on that date
            
        Example:
            >>> storage.get_instruments_active_on_date('2024-06-15')
        """
        sql = """
            SELECT i.*, l.first_trade_date, l.termination_date
            FROM instruments i
            LEFT JOIN listings l ON i.isin = l.isin
            WHERE l.first_trade_date <= ?
              AND (l.termination_date IS NULL OR l.termination_date >= ?)
        """
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.con.execute(sql, [target_date, target_date]).fetchdf()
    
    def get_instrument_state_on_date(self, isin: str, target_date: str) -> Optional[Dict[str, Any]]:
        """
        Get historical state of an instrument on a specific date.
        
        Per ESMA Section 9: Query historical state on date T.
        
        Args:
            isin: Instrument ISIN
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            Instrument record as it existed on that date, or None
            
        Example:
            >>> storage.get_instrument_state_on_date('GB00B1YW4409', '2023-06-15')
        """
        result = self.con.execute("""
            SELECT * FROM instrument_history
            WHERE isin = ?
              AND valid_from_date <= ?
              AND (valid_to_date IS NULL OR valid_to_date >= ?)
            ORDER BY version_number DESC
            LIMIT 1
        """, [isin, target_date, target_date]).fetchdf()
        
        if result.empty:
            # Fallback to current instruments table
            result = self.con.execute("""
                SELECT * FROM instruments
                WHERE isin = ?
                  AND valid_from_date <= ?
                  AND (valid_to_date IS NULL OR valid_to_date >= ?)
            """, [isin, target_date, target_date]).fetchdf()
        
        return result.iloc[0].to_dict() if not result.empty else None
    
    def get_instrument_version_history(self, isin: str) -> pd.DataFrame:
        """
        Get complete version history for an instrument.
        
        Args:
            isin: Instrument ISIN
            
        Returns:
            DataFrame with all versions ordered by version_number
            
        Example:
            >>> history = storage.get_instrument_version_history('GB00B1YW4409')
            >>> print(f"Versions: {len(history)}")
        """
        return self.con.execute("""
            SELECT * FROM instrument_history
            WHERE isin = ?
            ORDER BY version_number ASC
        """, [isin]).fetchdf()
    
    def get_modified_instruments_since(self, since_date: str) -> pd.DataFrame:
        """
        Get instruments modified since a specific date.
        
        Useful for tracking changes and delta processing.
        
        Args:
            since_date: Date in YYYY-MM-DD format
            
        Returns:
            DataFrame with modified instruments
        """
        return self.con.execute("""
            SELECT * FROM instruments
            WHERE valid_from_date >= ?
              AND record_type IN ('MODIFIED', 'NEW')
            ORDER BY valid_from_date DESC
        """, [since_date]).fetchdf()
    
    def get_cancelled_instruments(self, since_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get cancelled instruments from FULCAN files.
        
        Args:
            since_date: Optional date filter (YYYY-MM-DD)
            
        Returns:
            DataFrame with cancellation records
        """
        sql = "SELECT * FROM cancellations"
        params = []
        
        if since_date:
            sql += " WHERE cancellation_date >= ?"
            params.append(since_date)
        
        sql += " ORDER BY cancellation_date DESC"
        
        return self.con.execute(sql, params if params else None).fetchdf()
    
    def process_delta_record(self, isin: str, record_type: str, record_data: Dict[str, Any],
                            publication_date: str, source_file: str) -> Dict[str, str]:
        """
        Process a delta file record per ESMA Section 8.2 version management.
        
        Args:
            isin: Instrument ISIN
            record_type: NEW, MODIFIED, TERMINATED, or CANCELLED
            record_data: Instrument attributes
            publication_date: Publication date from file (becomes valid_from_date)
            source_file: Source file name
            
        Returns:
            Dictionary with status and message
            
        Example:
            >>> storage.process_delta_record(
            ...     isin='GB00B1YW4409',
            ...     record_type='MODIFIED',
            ...     record_data={...},
            ...     publication_date='2025-01-10',
            ...     source_file='DLTINS_S_20250110_01of01.zip'
            ... )
        """
        import json
        from datetime import datetime, timedelta
        
        if record_type == "NEW":
            # Insert new instrument version
            # Check if ISIN already exists (late record scenario)
            existing = self.con.execute("""
                SELECT version_number FROM instruments WHERE isin = ?
            """, [isin]).fetchone()
            
            if existing:
                # Late NEW record - existing instrument
                next_version = existing[0] + 1
                
                # Close previous version
                close_date = datetime.strptime(publication_date, '%Y-%m-%d') - timedelta(days=1)
                self.con.execute("""
                    UPDATE instruments
                    SET valid_to_date = ?,
                        latest_record_flag = FALSE
                    WHERE isin = ? AND latest_record_flag = TRUE
                """, [close_date.strftime('%Y-%m-%d'), isin])
                
                # Insert into history
                self.con.execute("""
                    INSERT INTO instrument_history 
                    (isin, version_number, valid_from_date, valid_to_date, record_type,
                     cfi_code, full_name, issuer, attributes, source_file, source_file_type, indexed_at)
                    SELECT isin, version_number, valid_from_date, ?, 'NEW',
                           cfi_code, full_name, issuer, ?::JSON, source_file, source_file_type, indexed_at
                    FROM instruments
                    WHERE isin = ?
                """, [close_date.strftime('%Y-%m-%d'), json.dumps(record_data), isin])
            else:
                # Truly new instrument
                next_version = 1
            
            # Insert new version into instruments table
            self.con.execute("""
                INSERT OR REPLACE INTO instruments 
                (isin, valid_from_date, valid_to_date, latest_record_flag, record_type,
                 version_number, source_file_type, last_update_timestamp, 
                 full_name, cfi_code, issuer)
                VALUES (?, ?, NULL, TRUE, ?, ?, 'DLTINS', ?, ?, ?, ?)
            """, [isin, publication_date, record_type, next_version,
                  datetime.now().isoformat(), 
                  record_data.get('full_name'),
                  record_data.get('cfi_code'),
                  record_data.get('issuer')])
            
            return {"status": "inserted", "message": f"NEW record for {isin}, version {next_version}"}
        
        elif record_type == "MODIFIED":
            # Close previous version and insert new one
            existing = self.con.execute("""
                SELECT version_number, valid_from_date 
                FROM instruments 
                WHERE isin = ? AND latest_record_flag = TRUE
            """, [isin]).fetchone()
            
            if not existing:
                return {"status": "error", "message": f"Cannot modify non-existent ISIN: {isin}"}
            
            current_version, prev_valid_from = existing
            next_version = current_version + 1
            
            # Close previous version (valid_to_date = new valid_from - 1 day)
            close_date = datetime.strptime(publication_date, '%Y-%m-%d') - timedelta(days=1)
            
            # Archive to history before updating
            self.con.execute("""
                INSERT INTO instrument_history
                (isin, version_number, valid_from_date, valid_to_date, record_type,
                 cfi_code, full_name, issuer, attributes, source_file, source_file_type, indexed_at)
                SELECT isin, version_number, valid_from_date, ?, record_type,
                       cfi_code, full_name, issuer, ?::JSON, source_file, source_file_type, indexed_at
                FROM instruments
                WHERE isin = ? AND latest_record_flag = TRUE
            """, [close_date.strftime('%Y-%m-%d'), json.dumps(record_data), isin])
            
            # Update current record with new version
            self.con.execute("""
                UPDATE instruments
                SET valid_from_date = ?,
                    valid_to_date = NULL,
                    latest_record_flag = TRUE,
                    record_type = ?,
                    version_number = ?,
                    source_file_type = 'DLTINS',
                    last_update_timestamp = ?,
                    full_name = ?,
                    cfi_code = ?,
                    issuer = ?
                WHERE isin = ?
            """, [publication_date, record_type, next_version,
                  datetime.now().isoformat(),
                  record_data.get('full_name'),
                  record_data.get('cfi_code'),
                  record_data.get('issuer'),
                  isin])
            
            return {"status": "updated", "message": f"MODIFIED record for {isin}, version {next_version}"}
        
        elif record_type == "TERMINATED":
            # Close instrument (set valid_to_date, mark not latest)
            existing = self.con.execute("""
                SELECT version_number FROM instruments WHERE isin = ? AND latest_record_flag = TRUE
            """, [isin]).fetchone()
            
            if not existing:
                return {"status": "error", "message": f"Cannot terminate non-existent ISIN: {isin}"}
            
            # Archive to history before termination
            self.con.execute("""
                INSERT INTO instrument_history
                (isin, version_number, valid_from_date, valid_to_date, record_type,
                 cfi_code, full_name, issuer, attributes, source_file, source_file_type, indexed_at)
                SELECT isin, version_number, valid_from_date, ?, 'TERMINATED',
                       cfi_code, full_name, issuer, ?::JSON, source_file, source_file_type, indexed_at
                FROM instruments
                WHERE isin = ? AND latest_record_flag = TRUE
            """, [publication_date, json.dumps(record_data), isin])
            
            # Mark as terminated
            self.con.execute("""
                UPDATE instruments
                SET valid_to_date = ?,
                    latest_record_flag = FALSE,
                    record_type = 'TERMINATED',
                    last_update_timestamp = ?
                WHERE isin = ? AND latest_record_flag = TRUE
            """, [publication_date, datetime.now().isoformat(), isin])
            
            return {"status": "terminated", "message": f"TERMINATED record for {isin}"}
        
        elif record_type == "CANCELLED":
            # Move to cancellations table and remove from instruments
            # Extract trading venue if available
            trading_venue = record_data.get('trading_venue_id', 'UNKNOWN')
            cancellation_reason = record_data.get('cancellation_reason', 'Not specified')
            
            # Get original publication date if available
            original_pub_date = self.con.execute("""
                SELECT MIN(valid_from_date) FROM instruments WHERE isin = ?
            """, [isin]).fetchone()
            
            original_pub_date_str = original_pub_date[0] if original_pub_date and original_pub_date[0] else None
            
            # Insert into cancellations
            self.con.execute("""
                INSERT INTO cancellations
                (isin, trading_venue_id, cancellation_date, cancellation_reason,
                 original_publication_date, source_file, indexed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [isin, trading_venue, publication_date, cancellation_reason,
                  original_pub_date_str, source_file, datetime.now().isoformat()])
            
            # Remove from instruments (optional - could also mark as cancelled)
            self.con.execute("""
                DELETE FROM instruments WHERE isin = ?
            """, [isin])
            
            return {"status": "cancelled", "message": f"CANCELLED record for {isin}, moved to cancellations"}
        
        else:
            return {"status": "error", "message": f"Unknown record type: {record_type}"}
    
    def close(self):
        """Close database connection."""
        if hasattr(self, 'con'):
            self.con.close()
