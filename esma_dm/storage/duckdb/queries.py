"""
DuckDB query and retrieval operations module.
"""

import logging
from typing import Dict, List, Optional, Any
import pandas as pd

from .connection import DuckDBConnection
from esma_dm.models.utils import decode_cfi
from esma_dm.utils import QueryBuilder


class DuckDBQueries:
    """Handles instrument queries and data retrieval operations."""
    
    def __init__(self, connection: DuckDBConnection):
        """Initialize queries with database connection."""
        self.connection = connection
        self.logger = logging.getLogger(__name__)
        self.query_builder = QueryBuilder(connection.mode)
    
    @property
    def con(self):
        """Get database connection."""
        self.connection._ensure_connection()
        return self.connection.con
    
    def get_instrument(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Get instrument by ISIN with comprehensive details.
        
        Args:
            isin: Instrument ISIN code
            
        Returns:
            Instrument details or None if not found
            
        Example:
            >>> instrument = queries.get_instrument('US0378331005')
            >>> print(instrument['full_name'])  # 'APPLE INC'
        """
        self.connection._ensure_connection()
        
        # Query master table first
        query = self.query_builder.get_instrument_by_isin(isin)
        cursor = self.con.execute(query, [isin])
        row = cursor.fetchone()
        
        if not row:
            return None
        
        # Build dict dynamically from column descriptions
        columns = [desc[0] for desc in cursor.description]
        instrument = dict(zip(columns, row))
        
        # Get CFI classification if available
        if instrument.get("cfi_code"):
            try:
                cfi_info = decode_cfi(instrument["cfi_code"])
                instrument["cfi_classification"] = {
                    "category": cfi_info.category,
                    "group": cfi_info.group,
                    **cfi_info.attributes
                }
            except Exception as e:
                self.logger.warning(f"Failed to parse CFI code {instrument['cfi_code']}: {e}")
        
        # Try to get asset-specific details
        asset_type = instrument.get("instrument_type") or (instrument["cfi_code"][0] if instrument.get("cfi_code") else None)
        
        if asset_type and self.query_builder.validate_asset_type(asset_type):
            try:
                # Get asset-specific attributes using QueryBuilder
                asset_query = self.query_builder.get_asset_specific_details(asset_type, isin)
                asset_result = self.con.execute(asset_query, [isin]).fetchone()
                
                if asset_result:
                    # Get column names
                    columns = [desc[0] for desc in self.con.description]
                    asset_data = dict(zip(columns, asset_result))
                    instrument["asset_specific"] = asset_data
                    
            except Exception as e:
                self.logger.debug(f"No asset-specific data found for {isin}: {e}")
        
        return instrument
    
    def get_stats_by_asset_type(self) -> Dict[str, int]:
        """Get count of instruments by asset type."""
        self.connection._ensure_connection()
        
        query = self.query_builder.get_stats_by_asset_type()
        result = self.con.execute(query).fetchall()
        
        return {row[0]: row[1] for row in result}
    
    def get_instrument_history(self, isin: str) -> List[Dict[str, Any]]:
        """
        Get version history for an instrument (history mode only).
        
        Args:
            isin: Instrument ISIN
            
        Returns:
            List of historical versions
        """
        if self.connection.mode != 'history':
            raise ValueError("Instrument history only available in history mode")
        
        self.connection._ensure_connection()
        
        # Get from both current and history tables
        current_result = self.con.execute("""
            SELECT isin, version_number, valid_from_date, valid_to_date, 
                   record_type, cfi_code, full_name, issuer,
                   source_file, indexed_at, latest_record_flag
            FROM instruments
            WHERE isin = ?
            ORDER BY version_number DESC
        """, [isin]).fetchall()
        
        history_result = self.con.execute("""
            SELECT isin, version_number, valid_from_date, valid_to_date,
                   record_type, cfi_code, full_name, issuer,
                   source_file, indexed_at
            FROM instrument_history
            WHERE isin = ?
            ORDER BY version_number DESC
        """, [isin]).fetchall()
        
        # Combine results
        all_versions = []
        
        for row in current_result:
            all_versions.append({
                "isin": row[0],
                "version_number": row[1],
                "valid_from_date": row[2],
                "valid_to_date": row[3],
                "record_type": row[4],
                "cfi_code": row[5],
                "full_name": row[6],
                "issuer": row[7],
                "source_file": row[8],
                "indexed_at": row[9],
                "latest_record_flag": row[10] if len(row) > 10 else None,
                "table_source": "current"
            })
        
        for row in history_result:
            all_versions.append({
                "isin": row[0],
                "version_number": row[1],
                "valid_from_date": row[2],
                "valid_to_date": row[3],
                "record_type": row[4],
                "cfi_code": row[5],
                "full_name": row[6],
                "issuer": row[7],
                "source_file": row[8],
                "indexed_at": row[9],
                "latest_record_flag": False,
                "table_source": "history"
            })
        
        # Sort by version number descending
        return sorted(all_versions, key=lambda x: x["version_number"], reverse=True)
    
    def search_instruments(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search instruments by name or ISIN.
        
        Args:
            query: Search query
            limit: Maximum results to return
            
        Returns:
            List of matching instruments
        """
        self.connection._ensure_connection()
        
        # Use QueryBuilder for search query and parameters
        sql_query = self.query_builder.search_instruments(limit)
        search_params = QueryBuilder.format_search_params(query)
        search_params.append(limit)  # Add limit parameter
        
        result = self.con.execute(sql_query, search_params).fetchall()
        
        return [
            {
                "isin": row[0],
                "cfi_code": row[1],
                "full_name": row[2],
                "issuer": row[3]
            } for row in result
        ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        self.connection._ensure_connection()
        
        stats = {}
        
        # Total instruments using QueryBuilder
        total_query = self.query_builder.get_total_instruments_count()
        total_result = self.con.execute(total_query).fetchone()
        stats["total_instruments"] = total_result[0] if total_result else 0
        
        # Total listings
        try:
            listings_result = self.con.execute("SELECT COUNT(*) FROM listings").fetchone()
            stats["total_listings"] = listings_result[0] if listings_result else 0
        except:
            stats["total_listings"] = 0
        
        # By asset type
        stats["by_asset_type"] = self.get_stats_by_asset_type()
        
        # Database info
        stats["database_path"] = self.connection.db_path
        stats["mode"] = self.connection.mode
        
        # Table sizes using QueryBuilder
        tables_query = self.query_builder.get_table_sizes()
        tables_result = self.con.execute(tables_query).fetchall()
        table_sizes = {}
        
        for table_row in tables_result:
            table_name = table_row[0]
            try:
                count_query = self.query_builder.get_table_count(table_name)
                count_result = self.con.execute(count_query).fetchone()
                table_sizes[table_name] = count_result[0] if count_result else 0
            except:
                table_sizes[table_name] = 0
        
        stats["table_sizes"] = table_sizes
        
        return stats
    
    def classify_instrument(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Get CFI-based classification for an instrument.
        
        Args:
            isin: Instrument ISIN
            
        Returns:
            Classification details or None if not found
        """
        instrument = self.get_instrument(isin)
        
        if not instrument or not instrument.get("cfi_code"):
            return None
        
        cfi_code = instrument["cfi_code"]
        
        try:
            cfi_info = decode_cfi(cfi_code)
            if not cfi_info:
                return None

            attr_vals = list(cfi_info.attributes.values())
            classification = {
                "isin": isin,
                "cfi_code": cfi_code,
                "category": cfi_info.category,
                "group": cfi_info.group,
                "attribute1": attr_vals[0] if len(attr_vals) > 0 else None,
                "attribute2": attr_vals[1] if len(attr_vals) > 1 else None,
                "full_name": instrument.get("full_name"),
                "issuer": instrument.get("issuer")
            }

            # Add human-readable descriptions from attributes dict
            classification["descriptions"] = {
                "category_desc": cfi_info.category,
                "group_desc": cfi_info.group,
                **cfi_info.attributes
            }

            return classification

        except Exception as e:
            self.logger.error(f"Failed to classify instrument {isin}: {e}")
            return None
    
    def get_instruments_by_cfi_category(self, category: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get instruments by CFI category.
        
        Args:
            category: CFI category (first character, e.g., 'E' for equities)
            limit: Maximum results to return
            
        Returns:
            List of instruments in the category
        """
        self.connection._ensure_connection()
        
        # Use QueryBuilder for CFI category query
        query = self.query_builder.get_instruments_by_cfi_category(limit)
        result = self.con.execute(query, [f"{category}%"]).fetchall()
        
        instruments = []
        for row in result:
            instrument = {
                "isin": row[0],
                "cfi_code": row[1],
                "full_name": row[2],
                "issuer": row[3]
            }
            
            # Add CFI classification
            try:
                cfi_info = decode_cfi(row[1])
                if cfi_info:
                    instrument["classification"] = {
                        "category": cfi_info.category,
                        "group": cfi_info.group
                    }
            except Exception:
                pass
                
            instruments.append(instrument)
        
        return instruments
    
    def get_latest_instruments(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get latest instruments from the database.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with latest instruments
        """
        self.connection._ensure_connection()
        
        sql = """
            SELECT isin, cfi_code, full_name, issuer, version_number, indexed_at
            FROM instruments
            ORDER BY indexed_at DESC
        """
        
        params = []
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        
        return self.con.execute(sql, params if params else None).fetchdf()
    
    def get_instruments_active_on_date(self, target_date: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Get instruments that were active on a specific date (history mode only).
        
        Args:
            target_date: Date in YYYY-MM-DD format
            limit: Maximum number of records to return
            
        Returns:
            DataFrame with instruments active on the target date
        """
        if self.connection.mode != 'history':
            raise ValueError("Historical date queries only available in history mode")
        
        self.connection._ensure_connection()
        
        sql = """
            SELECT isin, cfi_code, full_name, issuer, 
                   valid_from_date, valid_to_date, version_number
            FROM instruments
            WHERE valid_from_date <= ?
              AND (valid_to_date IS NULL OR valid_to_date >= ?)
            ORDER BY isin, version_number DESC
        """
        
        params = [target_date, target_date]
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        
        return self.con.execute(sql, params if params else None).fetchdf()
    
    def get_instrument_state_on_date(self, isin: str, target_date: str) -> Optional[Dict[str, Any]]:
        """
        Get instrument state as of a specific date (history mode only).
        
        Args:
            isin: Instrument ISIN
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            Instrument state on target date or None if not active
        """
        if self.connection.mode != 'history':
            raise ValueError("Historical state queries only available in history mode")
        
        self.connection._ensure_connection()
        
        # Find the version active on the target date
        result = self.con.execute("""
            SELECT isin, cfi_code, full_name, issuer,
                   valid_from_date, valid_to_date, version_number, record_type
            FROM instruments
            WHERE isin = ?
              AND valid_from_date <= ?
              AND (valid_to_date IS NULL OR valid_to_date >= ?)
            ORDER BY version_number DESC
            LIMIT 1
        """, [isin, target_date, target_date]).fetchone()
        
        if not result:
            return None
        
        return {
            "isin": result[0],
            "cfi_code": result[1],
            "full_name": result[2],
            "issuer": result[3],
            "valid_from_date": result[4],
            "valid_to_date": result[5],
            "version_number": result[6],
            "record_type": result[7],
            "query_date": target_date
        }
    
    def get_instrument_version_history(self, isin: str) -> pd.DataFrame:
        """
        Get complete version history for an instrument (history mode only).
        
        Args:
            isin: Instrument ISIN
            
        Returns:
            DataFrame with all versions of the instrument
        """
        if self.connection.mode != 'history':
            raise ValueError("Version history only available in history mode")
        
        self.connection._ensure_connection()
        
        sql = """
            SELECT isin, version_number, valid_from_date, valid_to_date,
                   record_type, cfi_code, full_name, issuer,
                   source_file, indexed_at, latest_record_flag,
                   'current' as table_source
            FROM instruments
            WHERE isin = ?
            
            UNION ALL
            
            SELECT isin, version_number, valid_from_date, valid_to_date,
                   record_type, cfi_code, full_name, issuer,
                   source_file, indexed_at, FALSE as latest_record_flag,
                   'history' as table_source
            FROM instrument_history
            WHERE isin = ?
            
            ORDER BY version_number DESC
        """
        
        return self.con.execute(sql, [isin, isin]).fetchdf()
    
    def get_modified_instruments_since(self, since_date: str) -> pd.DataFrame:
        """
        Get instruments modified since a specific date (history mode only).
        
        Args:
            since_date: Date in YYYY-MM-DD format
            
        Returns:
            DataFrame with modified instruments
        """
        if self.connection.mode != 'history':
            raise ValueError("Modification tracking only available in history mode")
        
        self.connection._ensure_connection()
        
        sql = """
            SELECT isin, version_number, valid_from_date, record_type,
                   cfi_code, full_name, issuer, source_file
            FROM instruments
            WHERE valid_from_date >= ?
              AND record_type IN ('MODIFIED', 'NEW')
            ORDER BY valid_from_date DESC, isin
        """
        
        return self.con.execute(sql, [since_date]).fetchdf()
    
    def get_cancelled_instruments(self, since_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get cancelled instruments (history mode only).
        
        Args:
            since_date: Optional date filter in YYYY-MM-DD format
            
        Returns:
            DataFrame with cancelled instruments
        """
        if self.connection.mode != 'history':
            raise ValueError("Cancellation tracking only available in history mode")
        
        self.connection._ensure_connection()
        
        sql = """
            SELECT isin, version_number, cancellation_date, 
                   cancellation_reason, cfi_code, full_name, issuer,
                   original_source_file, cancelled_by_file
            FROM cancellations
        """
        
        params = []
        if since_date:
            sql += " WHERE cancellation_date >= ?"
            params.append(since_date)
        
        sql += " ORDER BY cancellation_date DESC, isin"
        
        return self.con.execute(sql, params if params else None).fetchdf()