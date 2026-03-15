"""
DuckDB storage backend for FITRS transparency data.

FITRS tables (transparency, subclass_transparency, etc.) live in the same
unified database as FIRDS tables (esma_current.duckdb). Cross-dataset JOINs
between FIRDS instruments and FITRS transparency records work natively.
"""

from pathlib import Path
from typing import Optional, Dict, Any, List
import duckdb
import pandas as pd
from datetime import datetime

from ..schema.fitrs_schema import initialize_fitrs_schema, get_fitrs_schema_info


class FITRSStorage:
    """
    Storage backend for FITRS transparency data.

    FITRS tables live in the same unified DuckDB database as FIRDS tables.
    Can be constructed with a shared duckdb.Connection (from DuckDBStorage) to
    avoid opening a second handle to the same file, or with a db_path to open
    its own connection (useful when used standalone).
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        connection: Optional[duckdb.DuckDBPyConnection] = None,
        mode: str = 'current',
    ):
        """
        Initialize FITRS storage.

        Args:
            db_path: Path to the unified DuckDB file. When None the path is
                     resolved from Config (esma_dm/storage/duckdb/database/esma_{mode}.duckdb).
            connection: Existing duckdb.Connection to reuse (takes precedence over db_path).
            mode: Database mode ('current' or 'history'). Used only when db_path is None.
        """
        if connection is not None:
            # Share an existing connection — no need to open the file again.
            self.con = connection
            self.db_path = Path(connection.database) if hasattr(connection, 'database') else None
            self._owns_connection = False
        else:
            if db_path is None:
                from esma_dm.config import default_config
                db_path = str(default_config.get_database_path(mode))
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.con = duckdb.connect(str(self.db_path))
            self._owns_connection = True

        # Ensure FITRS tables exist (idempotent — CREATE TABLE IF NOT EXISTS).
        initialize_fitrs_schema(self.con)
    
    def initialize(self, mode: str = 'check', verify_only: bool = False) -> Dict[str, Any]:
        """
        Initialize or verify FITRS database schema.
        
        Args:
            mode: 'check' to verify existing, 'create' to initialize new
            verify_only: Only verify structure, don't create
            
        Returns:
            Dictionary with status information
        """
        if verify_only or mode == 'check':
            return self._verify_schema_structure()
        
        # Create/recreate schema
        initialize_fitrs_schema(self.con)
        return {
            'status': 'created',
            'tables': list(get_fitrs_schema_info().keys()),
            'database_path': str(self.db_path)
        }
    
    def _verify_schema_structure(self) -> Dict[str, Any]:
        """Verify schema structure matches expected."""
        schema_info = get_fitrs_schema_info()
        results = {}
        
        for table_name, expected in schema_info.items():
            try:
                result = self.con.execute(f"SELECT * FROM {table_name} LIMIT 0").fetchdf()
                actual_columns = set(result.columns)
                expected_columns = set(expected['columns'])
                
                results[table_name] = {
                    'exists': True,
                    'columns_match': actual_columns == expected_columns,
                    'missing_columns': list(expected_columns - actual_columns),
                    'extra_columns': list(actual_columns - expected_columns)
                }
            except Exception as e:
                results[table_name] = {
                    'exists': False,
                    'error': str(e)
                }
        
        return {
            'status': 'verified',
            'tables': results,
            'all_valid': all(t.get('exists') and t.get('columns_match', False) 
                           for t in results.values())
        }
    
    def drop(self, confirm: bool = False) -> Dict[str, Any]:
        """
        Drop FITRS database.
        
        Args:
            confirm: Must be True to actually drop
            
        Returns:
            Status information
        """
        if not confirm:
            return {
                'status': 'cancelled',
                'message': 'Must pass confirm=True to drop database'
            }
        
        # Get size before dropping
        size_bytes = self.db_path.stat().st_size if self.db_path.exists() else 0
        size_mb = size_bytes / (1024 * 1024)
        
        # Close connection
        self.con.close()
        
        # Remove file
        if self.db_path.exists():
            self.db_path.unlink()
        
        return {
            'status': 'dropped',
            'size_mb': round(size_mb, 2),
            'path': str(self.db_path)
        }
    
    def insert_transparency_data(self, df: pd.DataFrame, file_type: str) -> int:
        """
        Insert transparency data into database.
        
        Args:
            df: DataFrame with FITRS data
            file_type: 'FULECR', 'FULNCR', 'DLTECR', 'DLTNCR'
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            return 0
        
        instrument_type = 'equity' if 'ECR' in file_type else 'non_equity'
        
        # Map FITRS columns to database columns.
        # FULECR (equity) uses 'Id' for ISIN; FULNCR (non-equity) uses 'ISIN'.
        column_mapping = {
            'TechRcrdId': 'tech_record_id',
            # Equity (FULECR / DLTECR)
            'Id': 'isin',
            # Non-equity (FULNCR / DLTNCR) — ISIN key differs
            'ISIN': 'isin',
            'FinInstrmClssfctn': 'instrument_classification',
            'FrDt': 'reporting_period_from',
            'ToDt': 'reporting_period_to',
            'Mthdlgy': 'methodology',
            'TtlNbOfTxsExctd': 'total_number_transactions',
            'TtlVolOfTxsExctd': 'total_volume_transactions',
            'Lqdty': 'liquid_market',
            # Equity transparency metrics (FULECR)
            'AvrgDalyTrnvr': 'average_daily_turnover',
            'AvrgTxVal': 'average_transaction_value',
            'LrgInScale': 'large_in_scale',
            'StdMktSz': 'standard_market_size',
            'AvrgDalyNbOfTxs': 'average_daily_number_of_trades',
            'Id_2': 'most_relevant_market_id',
            'AvrgDalyNbOfTxs_3': 'most_relevant_market_avg_daily_trades',
            'Sttstcs': 'statistics',
            # Application period
            'ApplFrDt': 'application_period_from',
            'ApplToDt': 'application_period_to',
            # Non-equity thresholds — named fields from XML-parsed files
            'PreTradLrgInScaleThrshld': 'pre_trade_lis_threshold',
            'PstTradLrgInScaleThrshld': 'post_trade_lis_threshold',
            'PreTradInstrmSzSpcfcThrshld': 'pre_trade_ssti_threshold',
            'PstTradInstrmSzSpcfcThrshld': 'post_trade_ssti_threshold',
            # Flattened non-equity thresholds as they appear in CSV-parsed files
            # Amt_EUR = pre-trade LIS, Amt_EUR_4 = post-trade LIS, Amt_EUR_2 = pre-trade SSTI
            'Amt_EUR': 'pre_trade_lis_threshold',
            'Amt_EUR_4': 'post_trade_lis_threshold',
            'Amt_EUR_2': 'pre_trade_ssti_threshold',
        }
        
        # Rename columns
        main_df = df.rename(columns=column_mapping).copy()
        
        # Add metadata
        main_df['instrument_type'] = instrument_type
        main_df['file_type'] = file_type
        
        # Convert boolean liquid_market — handle XML lowercase booleans and ESMA codes
        if 'liquid_market' in main_df.columns:
            def _to_bool(val):
                if isinstance(val, bool):
                    return val
                if isinstance(val, str):
                    return val.lower() in ('true', '1', 'lqdt', 'yes')
                return None
            main_df['liquid_market'] = main_df['liquid_market'].apply(_to_bool)

        # Select only columns that exist in the schema
        db_columns = [
            'tech_record_id', 'isin', 'instrument_classification', 'instrument_type',
            'reporting_period_from', 'reporting_period_to',
            'application_period_from', 'application_period_to',
            'methodology', 'total_number_transactions', 'total_volume_transactions',
            'liquid_market', 'average_daily_turnover', 'average_transaction_value',
            'standard_market_size', 'average_daily_number_of_trades',
            'most_relevant_market_id', 'most_relevant_market_avg_daily_trades',
            'pre_trade_lis_threshold', 'post_trade_lis_threshold',
            'pre_trade_ssti_threshold', 'post_trade_ssti_threshold',
            'large_in_scale', 'statistics', 'file_type', 'file_date'
        ]
        
        main_df = main_df[[col for col in db_columns if col in main_df.columns]]
        
        # Build explicit column list for INSERT (exclude surrogate 'id' — auto-generated by sequence)
        main_df = main_df[[c for c in main_df.columns if c != 'id']]
        column_list = ', '.join(main_df.columns)
        
        # Plain INSERT — table is always cleared before re-indexing so no conflicts expected
        self.con.execute(f"INSERT INTO transparency ({column_list}) SELECT * FROM main_df")
        
        # Create simple record for type-specific table (just ISIN link).
        # FULECR uses 'Id'; FULNCR uses 'ISIN' — handle both.
        isin_col = 'Id' if 'Id' in df.columns else 'ISIN'
        type_df = pd.DataFrame({'isin': df[isin_col].dropna().unique()})

        if instrument_type == 'equity':
            self.con.execute("INSERT OR IGNORE INTO equity_transparency (isin) SELECT * FROM type_df")
        elif instrument_type == 'non_equity':
            self.con.execute("INSERT OR IGNORE INTO non_equity_transparency (isin) SELECT * FROM type_df")
        
        return len(df)
    
    def insert_subclass_transparency_data(self, df: pd.DataFrame, file_type: str) -> int:
        """
        Insert sub-class transparency data (FULNCR_NYAR, FULNCR_SISC).
        
        Args:
            df: DataFrame with sub-class transparency data
            file_type: 'FULNCR_NYAR' or 'FULNCR_SISC'
            
        Returns:
            Number of records inserted
        """
        if df.empty:
            return 0
        
        # Map FITRS sub-class columns to database columns
        column_mapping = {
            'TechRcrdId': 'tech_record_id',
            'AsstClss': 'asset_class',
            'SubAsstClssCd': 'sub_asset_class_code',
            'SubAsstClssDesc': 'sub_asset_class_description',
            'SgmnttnCriteria': 'segmentation_criteria',
            'ClctnTp': 'calculation_type',
            'Mthdlgy': 'methodology',
            'FrDt': 'reporting_period_from',
            'ToDt': 'reporting_period_to',
            'ApplFrDt': 'application_period_from',
            'ApplToDt': 'application_period_to',
            'Lqdty': 'liquid_market',
            'TtlNbOfTxsExctd': 'total_number_transactions',
            'TtlVolOfTxsExctd': 'total_volume_transactions',
            'AvrgDalyTrnvr': 'average_daily_turnover',
            'PreTradLrgInScaleThrshld': 'pre_trade_lis_threshold',
            'PstTradLrgInScaleThrshld': 'post_trade_lis_threshold',
            'PreTradInstrmSzSpcfcThrshld': 'pre_trade_ssti_threshold',
            'PstTradInstrmSzSpcfcThrshld': 'post_trade_ssti_threshold'
        }
        
        # Rename columns
        main_df = df.rename(columns=column_mapping).copy()
        
        # Add metadata
        main_df['file_type'] = file_type
        
        # Convert boolean liquid_market — handle XML lowercase booleans and ESMA codes
        if 'liquid_market' in main_df.columns:
            def _to_bool_sub(val):
                if isinstance(val, bool):
                    return val
                if isinstance(val, str):
                    return val.lower() in ('true', '1', 'lqdt', 'yes')
                return None
            main_df['liquid_market'] = main_df['liquid_market'].apply(_to_bool_sub)

        # Parse segmentation_criteria as JSON if string
        if 'segmentation_criteria' in main_df.columns:
            import json
            main_df['segmentation_criteria'] = main_df['segmentation_criteria'].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )
        
        # Select only columns that exist in the schema
        db_columns = [
            'tech_record_id', 'asset_class', 'sub_asset_class_code', 
            'sub_asset_class_description', 'segmentation_criteria',
            'calculation_type', 'methodology',
            'reporting_period_from', 'reporting_period_to',
            'application_period_from', 'application_period_to',
            'liquid_market', 'total_number_transactions', 'total_volume_transactions',
            'average_daily_turnover',
            'pre_trade_lis_threshold', 'post_trade_lis_threshold',
            'pre_trade_ssti_threshold', 'post_trade_ssti_threshold',
            'file_type', 'file_date'
        ]
        
        main_df = main_df[[col for col in db_columns if col in main_df.columns]]
        
        # Build explicit column list for INSERT
        column_list = ', '.join(main_df.columns)
        
        # Insert into subclass_transparency table
        self.con.execute(
            f"INSERT OR REPLACE INTO subclass_transparency ({column_list}) SELECT * FROM main_df"
        )
        
        return len(df)
    
    def get_transparency(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Get transparency data for an ISIN.
        
        Args:
            isin: ISIN code
            
        Returns:
            Dictionary with transparency data or None if not found
        """
        result = self.con.execute("""
            SELECT * FROM transparency WHERE isin = ?
        """, [isin]).fetchdf()
        
        if result.empty:
            return None
        
        return result.iloc[0].to_dict()
    
    def query(self, sql: str, params: Optional[List] = None) -> pd.DataFrame:
        """
        Execute SQL query on FITRS database.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            Query results as DataFrame
        """
        if params:
            return self.con.execute(sql, params).fetchdf()
        return self.con.execute(sql).fetchdf()
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with statistics
        """
        stats = {}
        
        # Count records
        result = self.con.execute("SELECT COUNT(*) FROM transparency").fetchone()
        stats['total_instruments'] = result[0] if result else 0
        
        result = self.con.execute(
            "SELECT COUNT(DISTINCT instrument_classification) FROM transparency"
        ).fetchone()
        stats['instrument_classifications'] = result[0] if result else 0
        
        result = self.con.execute(
            "SELECT COUNT(*) FROM transparency WHERE liquid_market = TRUE"
        ).fetchone()
        stats['liquid_instruments'] = result[0] if result else 0
        
        result = self.con.execute(
            "SELECT COUNT(*) FROM transparency WHERE liquid_market = FALSE"
        ).fetchone()
        stats['illiquid_instruments'] = result[0] if result else 0
        
        # Database size
        if self.db_path.exists():
            size_bytes = self.db_path.stat().st_size
            stats['database_size_mb'] = round(size_bytes / (1024 * 1024), 2)
        
        return stats
    
    # attach_firds_database is no longer needed — FIRDS and FITRS tables
    # live in the same unified database. Cross-dataset JOINs work directly.
    # Example: SELECT i.full_name, t.liquid_market FROM instruments i
    #          JOIN transparency t ON i.isin = t.isin

    def close(self):
        """Close database connection (only if this instance owns it)."""
        if self._owns_connection and self.con:
            self.con.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
