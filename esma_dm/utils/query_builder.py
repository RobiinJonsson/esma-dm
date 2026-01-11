"""
Query builder utility for common ESMA database operations.

Provides reusable SQL query patterns for DuckDB operations including:
- CRUD operations with standardized patterns
- Asset type filtering and classification
- Historical version management
- Search and filtering patterns
- Statistics and aggregation queries
"""

import logging
from typing import Dict, List, Optional, Any, Union
from enum import Enum


class QueryMode(Enum):
    """Database mode for query selection."""
    CURRENT = 'current'
    HISTORY = 'history'


class QueryBuilder:
    """
    Builder for common ESMA database query patterns.
    
    Centralizes SQL construction to avoid code duplication across storage modules
    and provides consistent query patterns for instrument operations.
    """
    
    def __init__(self, mode: str = 'current'):
        """
        Initialize query builder.
        
        Args:
            mode: Database mode ('current' or 'history')
        """
        self.mode = mode
        self.logger = logging.getLogger(__name__)
    
    # Table name mappings
    ASSET_TYPE_TABLES = {
        'E': 'equity_instruments',
        'D': 'debt_instruments', 
        'F': 'futures_instruments',
        'O': 'option_instruments',
        'S': 'swap_instruments',
        'H': 'forward_instruments',
        'R': 'rights_instruments',
        'C': 'civ_instruments',
        'I': 'spot_instruments',
        'J': 'collective_investment_instruments'
    }
    
    # Base instrument query fields by mode
    BASE_FIELDS_CURRENT = """
        isin, cfi_code, full_name, issuer, source_file, indexed_at, asset_type
    """
    
    BASE_FIELDS_HISTORY = """
        isin, version_number, valid_from_date, valid_to_date, record_type,
        cfi_code, full_name, issuer, source_file, indexed_at, latest_record_flag
    """
    
    def get_instrument_by_isin(self, isin: str) -> str:
        """
        Build query to get instrument by ISIN.
        
        Args:
            isin: Instrument ISIN code
            
        Returns:
            SQL query string with parameter placeholder
        """
        if self.mode == 'history':
            return f"""
                SELECT {self.BASE_FIELDS_HISTORY}
                FROM instruments 
                WHERE isin = ? AND latest_record_flag = true
            """
        else:
            return f"""
                SELECT {self.BASE_FIELDS_CURRENT}
                FROM instruments 
                WHERE isin = ?
            """
    
    def get_instrument_history(self, isin: str) -> str:
        """
        Build query to get full instrument version history.
        
        Args:
            isin: Instrument ISIN code
            
        Returns:
            SQL query string with parameter placeholder
        """
        if self.mode != 'history':
            raise ValueError("History queries only available in history mode")
        
        return """
            SELECT isin, version_number, valid_from_date, valid_to_date,
                   record_type, cfi_code, full_name, issuer,
                   source_file, indexed_at
            FROM instruments
            WHERE isin = ?
            ORDER BY version_number DESC
        """
    
    def get_asset_specific_details(self, asset_type: str, isin: str) -> str:
        """
        Build query for asset-specific table details.
        
        Args:
            asset_type: CFI first character (E, D, O, etc.)
            isin: Instrument ISIN code
            
        Returns:
            SQL query string with parameter placeholder
        """
        table_name = self.ASSET_TYPE_TABLES.get(asset_type)
        if not table_name:
            raise ValueError(f"Unknown asset type: {asset_type}")
        
        return f"""
            SELECT * FROM {table_name}
            WHERE isin = ?
        """
    
    def search_instruments(self, limit: int = 10) -> str:
        """
        Build search query for instruments by name or ISIN.
        
        Args:
            limit: Maximum results to return
            
        Returns:
            SQL query with search ranking and limit
        """
        return f"""
            SELECT isin, cfi_code, full_name, issuer
            FROM instruments
            WHERE full_name ILIKE ? OR isin ILIKE ?
            ORDER BY 
                CASE 
                    WHEN isin = ? THEN 1
                    WHEN isin ILIKE ? THEN 2
                    WHEN full_name ILIKE ? THEN 3
                    ELSE 4
                END,
                full_name
            LIMIT {limit}
        """
    
    def get_instruments_by_cfi_category(self, limit: int = 100) -> str:
        """
        Build query for instruments by CFI category.
        
        Args:
            limit: Maximum results to return
            
        Returns:
            SQL query with CFI pattern matching
        """
        return f"""
            SELECT isin, cfi_code, full_name, issuer
            FROM instruments
            WHERE cfi_code LIKE ?
            ORDER BY full_name
            LIMIT {limit}
        """
    
    def get_total_instruments_count(self) -> str:
        """Build query for total instrument count."""
        return "SELECT COUNT(*) FROM instruments"
    
    def get_stats_by_asset_type(self) -> str:
        """
        Build query for instrument counts by asset type.
        
        Returns:
            SQL query that groups by CFI first character
        """
        return """
            SELECT 
                LEFT(cfi_code, 1) as asset_type,
                COUNT(*) as count
            FROM instruments 
            WHERE cfi_code IS NOT NULL 
            GROUP BY LEFT(cfi_code, 1)
            ORDER BY count DESC
        """
    
    def get_table_sizes(self) -> str:
        """Build query to show all tables."""
        return "SHOW TABLES"
    
    def get_table_count(self, table_name: str) -> str:
        """
        Build query for table row count.
        
        Args:
            table_name: Name of table to count
            
        Returns:
            SQL query for table count
        """
        return f"SELECT COUNT(*) FROM {table_name}"
    
    def bulk_insert_instruments(self, columns: List[str]) -> str:
        """
        Build bulk insert query for instruments table.
        
        Args:
            columns: List of column names for insert
            
        Returns:
            SQL INSERT query with parameterized values
        """
        placeholders = ", ".join("?" for _ in columns)
        column_list = ", ".join(columns)
        
        return f"""
            INSERT INTO instruments ({column_list})
            VALUES ({placeholders})
        """
    
    def bulk_insert_asset_table(self, asset_type: str, columns: List[str]) -> str:
        """
        Build bulk insert query for asset-specific table.
        
        Args:
            asset_type: CFI first character (E, D, O, etc.)
            columns: List of column names for insert
            
        Returns:
            SQL INSERT query with parameterized values
        """
        table_name = self.ASSET_TYPE_TABLES.get(asset_type)
        if not table_name:
            raise ValueError(f"Unknown asset type: {asset_type}")
        
        placeholders = ", ".join("?" for _ in columns)
        column_list = ", ".join(columns)
        
        return f"""
            INSERT INTO {table_name} ({column_list})
            VALUES ({placeholders})
        """
    
    def drop_table_if_exists(self, table_name: str) -> str:
        """
        Build DROP TABLE IF EXISTS query.
        
        Args:
            table_name: Name of table to drop
            
        Returns:
            SQL DROP query
        """
        return f"DROP TABLE IF EXISTS {table_name}"
    
    def create_index(self, table_name: str, column_name: str, index_name: Optional[str] = None) -> str:
        """
        Build CREATE INDEX query.
        
        Args:
            table_name: Table to create index on
            column_name: Column to index
            index_name: Custom index name (optional)
            
        Returns:
            SQL CREATE INDEX query
        """
        if index_name is None:
            index_name = f"idx_{table_name}_{column_name}"
        
        return f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_name})"
    
    def upsert_instrument(self, columns: List[str]) -> str:
        """
        Build UPSERT (INSERT OR REPLACE) query for instruments.
        
        Args:
            columns: List of column names
            
        Returns:
            SQL UPSERT query
        """
        placeholders = ", ".join("?" for _ in columns)
        column_list = ", ".join(columns)
        
        return f"""
            INSERT OR REPLACE INTO instruments ({column_list})
            VALUES ({placeholders})
        """
    
    def get_latest_version(self, isin: str) -> str:
        """
        Build query to get latest version number for an ISIN.
        
        Args:
            isin: Instrument ISIN code
            
        Returns:
            SQL query for max version number
        """
        return """
            SELECT MAX(version_number) 
            FROM instruments 
            WHERE isin = ?
        """
    
    def update_previous_versions(self, isin: str) -> str:
        """
        Build query to mark previous versions as not latest.
        
        Args:
            isin: Instrument ISIN code
            
        Returns:
            SQL UPDATE query
        """
        return """
            UPDATE instruments 
            SET latest_record_flag = false,
                valid_to_date = ?
            WHERE isin = ? AND latest_record_flag = true
        """
    
    def get_instruments_by_date_range(self, start_date: str, end_date: str) -> str:
        """
        Build query for instruments within date range.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            SQL query with date filtering
        """
        if self.mode == 'history':
            return """
                SELECT isin, cfi_code, full_name, issuer, valid_from_date, valid_to_date
                FROM instruments
                WHERE valid_from_date >= ? AND valid_from_date <= ?
                ORDER BY valid_from_date DESC
            """
        else:
            return """
                SELECT isin, cfi_code, full_name, issuer, indexed_at
                FROM instruments
                WHERE DATE(indexed_at) >= ? AND DATE(indexed_at) <= ?
                ORDER BY indexed_at DESC
            """
    
    @classmethod
    def format_search_params(cls, query: str) -> List[str]:
        """
        Format search query into parameter list for search queries.
        
        Args:
            query: User search term
            
        Returns:
            List of formatted parameters for SQL search
        """
        return [f"%{query}%", f"%{query}%", query, f"{query}%", f"{query}%"]
    
    @classmethod
    def get_asset_type_from_cfi(cls, cfi_code: str) -> Optional[str]:
        """
        Extract asset type from CFI code.
        
        Args:
            cfi_code: 6-character CFI code
            
        Returns:
            Asset type (first character) or None if invalid
        """
        if not cfi_code or len(cfi_code) < 1:
            return None
        return cfi_code[0].upper()
    
    @classmethod
    def validate_asset_type(cls, asset_type: str) -> bool:
        """
        Validate asset type code.
        
        Args:
            asset_type: Asset type to validate
            
        Returns:
            True if valid asset type
        """
        return asset_type in cls.ASSET_TYPE_TABLES