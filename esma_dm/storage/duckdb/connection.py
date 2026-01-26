"""
DuckDB connection and initialization module.
"""

import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional
import duckdb

from ..base import StorageBackend
from ..schema import initialize_schema
from esma_dm.config import get_database_config
from esma_dm import config as global_config


class DuckDBConnection:
    """Handles DuckDB connection and database initialization."""
    
    def __init__(self, db_path: str, mode: str = 'current'):
        """Initialize DuckDB connection manager."""
        self.logger = logging.getLogger(__name__)
        self.mode = mode
        self.db_path = db_path
        self.db_config = get_database_config(mode)
        
        self.con = None
    
    def _ensure_connection(self):
        """Ensure database connection is active."""
        if self.con is None:
            self.con = duckdb.connect(self.db_path)
    
    def initialize(self, mode: Optional[str] = None, verify_only: bool = False):
        """
        Initialize database schema based on mode.
        
        Args:
            mode: Database mode ('current' or 'history')
            verify_only: If True, only verify schema without creating tables
            
        Returns:
            Dict with initialization details and stats
        """
        self._ensure_connection()
        
        if mode is None:
            mode = self.mode
        
        # Enhanced logging
        start_time = time.time()
        self.logger.info(f"Initializing DuckDB storage in {mode} mode at {self.db_path}")
        
        try:
            # Initialize schema based on mode
            initialize_schema(self.con)
            result = {"status": "initialized", "mode": mode}
            
            duration = time.time() - start_time
            
            self.logger.info(f"Schema initialized in {duration:.2f}s")
            
            # Get basic stats
            try:
                stats = self._get_basic_stats()
            except Exception as e:
                self.logger.warning(f"Could not fetch stats: {e}")
                stats = {}
            
            return {
                "status": "initialized",
                "mode": mode,
                "duration_seconds": duration,
                "database_path": self.db_path,
                "existing_instruments": stats.get('instrument_count', 0),
                    "schema_info": result
                }
                
        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}")
            raise
    
    def _verify_schema_structure(self) -> Dict[str, Any]:
        """Verify schema structure and return detailed information."""
        self._ensure_connection()
        
        # Get all tables
        tables_result = self.con.execute("SHOW TABLES").fetchall()
        tables = [row[0] for row in tables_result]
        
        schema_info = {
            "tables": {},
            "table_count": len(tables),
            "mode": self.mode
        }
        
        for table_name in sorted(tables):
            try:
                # Get columns for each table
                columns_result = self.con.execute(f"DESCRIBE {table_name}").fetchall()
                columns = [{"name": col[0], "type": col[1]} for col in columns_result]
                
                # Get row count
                try:
                    count_result = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    row_count = count_result[0] if count_result else 0
                except:
                    row_count = 0
                
                schema_info["tables"][table_name] = {
                    "columns": columns,
                    "column_count": len(columns),
                    "row_count": row_count
                }
                
                self.logger.debug(f"Table {table_name}: {len(columns)} columns, {row_count} rows")
                
            except Exception as e:
                self.logger.error(f"Error analyzing table {table_name}: {e}")
                schema_info["tables"][table_name] = {"error": str(e)}
        
        # Check for mode-specific requirements
        required_tables = ["instruments"]
        
        if self.mode == 'history':
            required_tables.extend(["instrument_history", "cancellations"])
        
        missing_tables = [table for table in required_tables if table not in tables]
        if missing_tables:
            schema_info["missing_required_tables"] = missing_tables
            self.logger.warning(f"Missing required tables for {self.mode} mode: {missing_tables}")
        else:
            self.logger.info(f"All required tables present for {self.mode} mode")
        
        return schema_info
    
    def drop(self, confirm: bool = False):
        """
        Drop the database file.
        
        Args:
            confirm: Must be True to actually drop the database
            
        Raises:
            ValueError: If confirm is False
        """
        if not confirm:
            raise ValueError("Must explicitly confirm database drop with confirm=True")
        
        if self.con:
            self.con.close()
            self.con = None
        
        db_file = Path(self.db_path)
        if db_file.exists():
            db_file.unlink()
            self.logger.info(f"Dropped database: {self.db_path}")
            return {"status": "dropped", "database_path": self.db_path}
        else:
            self.logger.warning(f"Database file not found: {self.db_path}")
            return {"status": "not_found", "database_path": self.db_path}
    
    def _get_basic_stats(self) -> Dict[str, int]:
        """Get basic database statistics."""
        try:
            instrument_count = self.con.execute("SELECT COUNT(*) FROM instruments").fetchone()
            return {
                "instrument_count": instrument_count[0] if instrument_count else 0
            }
        except Exception:
            return {}
    
    def close(self):
        """Close database connection."""
        if self.con:
            self.con.close()
            self.con = None