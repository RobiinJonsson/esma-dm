"""
DuckDB storage backend for FITRS transparency data.

Manages a separate fitrs.db database that can be attached to firds.db
for cross-database queries.
"""

from pathlib import Path
from typing import Optional, Dict, Any, List
import duckdb
import pandas as pd
from datetime import datetime

from ..schema.fitrs_schema import initialize_fitrs_schema, get_fitrs_schema_info


class FITRSStorage:
    """
    Storage backend for FITRS transparency data using DuckDB.
    
    Creates a separate fitrs.db database that can be queried independently
    or joined with firds.db for combined queries.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize FITRS storage.
        
        Args:
            db_path: Path to fitrs.db file. Defaults to downloads/fitrs.db
        """
        if db_path is None:
            db_path = str(Path(__file__).parent.parent.parent / 'downloads' / 'fitrs.db')
        
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.con = duckdb.connect(str(self.db_path))
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Initialize database schema if needed."""
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
        
        # Map FITRS columns to database columns
        column_mapping = {
            'TechRcrdId': 'tech_record_id',
            'Id': 'isin',
            'FinInstrmClssfctn': 'instrument_classification',
            'FrDt': 'reporting_period_from',
            'ToDt': 'reporting_period_to',
            'Mthdlgy': 'methodology',
            'TtlNbOfTxsExctd': 'total_number_transactions',
            'TtlVolOfTxsExctd': 'total_volume_transactions',
            'Lqdty': 'liquid_market',
            'AvrgDalyTrnvr': 'average_daily_turnover',
            'AvrgTxVal': 'average_transaction_value',
            'LrgInScale': 'large_in_scale',
            'StdMktSz': 'standard_market_size',
            'AvrgDalyNbOfTxs': 'average_daily_number_of_trades',
            'Id_2': 'most_relevant_market_id',
            'AvrgDalyNbOfTxs_3': 'most_relevant_market_avg_daily_trades',
            'Sttstcs': 'statistics',
            # Application period (from April 2025 files)
            'ApplFrDt': 'application_period_from',
            'ApplToDt': 'application_period_to',
            # Non-equity thresholds
            'PreTradLrgInScaleThrshld': 'pre_trade_lis_threshold',
            'PstTradLrgInScaleThrshld': 'post_trade_lis_threshold',
            'PreTradInstrmSzSpcfcThrshld': 'pre_trade_ssti_threshold',
            'PstTradInstrmSzSpcfcThrshld': 'post_trade_ssti_threshold'
        }
        
        # Rename columns
        main_df = df.rename(columns=column_mapping).copy()
        
        # Add metadata
        main_df['instrument_type'] = instrument_type
        main_df['file_type'] = file_type
        
        # Convert boolean liquid_market
        if 'liquid_market' in main_df.columns:
            main_df['liquid_market'] = main_df['liquid_market'].map({
                'True': True, 'False': False, True: True, False: False
            })
        
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
        
        # Build explicit column list for INSERT
        column_list = ', '.join(main_df.columns)
        
        # Insert into main transparency table with explicit columns
        self.con.execute(f"INSERT OR REPLACE INTO transparency ({column_list}) SELECT * FROM main_df")
        
        # Create simple record for type-specific table (just ISIN link)
        type_df = pd.DataFrame({'isin': df['Id'].unique()})
        
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
        
        # Convert boolean liquid_market
        if 'liquid_market' in main_df.columns:
            main_df['liquid_market'] = main_df['liquid_market'].map({
                'True': True, 'False': False, True: True, False: False
            })
        
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
    
    def attach_firds_database(self, firds_db_path: str) -> None:
        """
        Attach FIRDS database for cross-database queries.
        
        Args:
            firds_db_path: Path to firds.db
            
        Example:
            >>> fitrs_store.attach_firds_database('storage/duckdb/database/firds_current.duckdb')
            >>> # Now can query both databases
            >>> result = fitrs_store.query(\"\"\"
            ...     SELECT f.isin, f.full_name, t.liquid_market, t.average_daily_turnover
            ...     FROM firds.instruments f
            ...     JOIN transparency t ON f.isin = t.isin
            ...     WHERE t.liquid_market = 'Y'
            ... \"\"\")
        """
        self.con.execute(f"ATTACH '{firds_db_path}' AS firds")
    
    def close(self):
        """Close database connection."""
        if self.con:
            self.con.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
