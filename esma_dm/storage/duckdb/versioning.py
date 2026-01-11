"""
DuckDB delta processing and version management module (history mode only).
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from .connection import DuckDBConnection


class DuckDBVersioning:
    """Handles delta file processing and version management for history mode."""
    
    def __init__(self, connection: DuckDBConnection):
        """Initialize versioning with database connection."""
        if connection.mode != 'history':
            raise ValueError("Versioning only available in history mode")
        
        self.connection = connection
        self.logger = logging.getLogger(__name__)
    
    @property
    def con(self):
        """Get database connection."""
        self.connection._ensure_connection()
        return self.connection.con
    
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
            >>> versioning.process_delta_record(
            ...     isin='GB00B1YW4409',
            ...     record_type='MODIFIED',
            ...     record_data={...},
            ...     publication_date='2025-01-10',
            ...     source_file='DLTINS_S_20250110_01of01.zip'
            ... )
        """
        self.connection._ensure_connection()
        
        if record_type == "NEW":
            return self._process_new_record(isin, record_data, publication_date, source_file)
        elif record_type == "MODIFIED":
            return self._process_modified_record(isin, record_data, publication_date, source_file)
        elif record_type == "TERMINATED":
            return self._process_terminated_record(isin, record_data, publication_date, source_file)
        elif record_type == "CANCELLED":
            return self._process_cancelled_record(isin, record_data, publication_date, source_file)
        else:
            return {"status": "error", "message": f"Unknown record type: {record_type}"}
    
    def _process_new_record(self, isin: str, record_data: Dict[str, Any], 
                           publication_date: str, source_file: str) -> Dict[str, str]:
        """Process NEW record type."""
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
    
    def _process_modified_record(self, isin: str, record_data: Dict[str, Any],
                                publication_date: str, source_file: str) -> Dict[str, str]:
        """Process MODIFIED record type."""
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
        
        # Update instruments table with new version
        self.con.execute("""
            UPDATE instruments
            SET version_number = ?,
                valid_from_date = ?,
                valid_to_date = NULL,
                record_type = ?,
                last_update_timestamp = ?,
                full_name = ?,
                cfi_code = ?,
                issuer = ?,
                source_file = ?
            WHERE isin = ? AND latest_record_flag = TRUE
        """, [next_version, publication_date, record_type, datetime.now().isoformat(),
              record_data.get('full_name'), record_data.get('cfi_code'),
              record_data.get('issuer'), source_file, isin])
        
        return {"status": "modified", "message": f"MODIFIED record for {isin}, version {next_version}"}
    
    def _process_terminated_record(self, isin: str, record_data: Dict[str, Any],
                                  publication_date: str, source_file: str) -> Dict[str, str]:
        """Process TERMINATED record type."""
        existing = self.con.execute("""
            SELECT version_number FROM instruments WHERE isin = ? AND latest_record_flag = TRUE
        """, [isin]).fetchone()
        
        if not existing:
            return {"status": "error", "message": f"Cannot terminate non-existent ISIN: {isin}"}
        
        # Archive current version to history
        self.con.execute("""
            INSERT INTO instrument_history
            (isin, version_number, valid_from_date, valid_to_date, record_type,
             cfi_code, full_name, issuer, attributes, source_file, source_file_type, indexed_at)
            SELECT isin, version_number, valid_from_date, ?, 'TERMINATED',
                   cfi_code, full_name, issuer, ?::JSON, source_file, source_file_type, indexed_at
            FROM instruments
            WHERE isin = ? AND latest_record_flag = TRUE
        """, [publication_date, json.dumps(record_data), isin])
        
        # Remove from active instruments
        self.con.execute("""
            DELETE FROM instruments WHERE isin = ? AND latest_record_flag = TRUE
        """, [isin])
        
        return {"status": "terminated", "message": f"TERMINATED record for {isin}"}
    
    def _process_cancelled_record(self, isin: str, record_data: Dict[str, Any],
                                 publication_date: str, source_file: str) -> Dict[str, str]:
        """Process CANCELLED record type."""
        # Find the instrument to cancel
        existing = self.con.execute("""
            SELECT version_number, cfi_code, full_name, issuer, source_file
            FROM instruments 
            WHERE isin = ? AND latest_record_flag = TRUE
        """, [isin]).fetchone()
        
        if not existing:
            return {"status": "error", "message": f"Cannot cancel non-existent ISIN: {isin}"}
        
        version_number, cfi_code, full_name, issuer, original_source = existing
        
        # Insert into cancellations table
        self.con.execute("""
            INSERT INTO cancellations
            (isin, version_number, cancellation_date, cancellation_reason,
             cfi_code, full_name, issuer, original_source_file, cancelled_by_file, indexed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [isin, version_number, publication_date, 
              record_data.get('cancellation_reason', 'Cancelled via delta file'),
              cfi_code, full_name, issuer, original_source, source_file, datetime.now().isoformat()])
        
        # Remove from active instruments
        self.con.execute("""
            DELETE FROM instruments WHERE isin = ? AND latest_record_flag = TRUE
        """, [isin])
        
        # Also remove from history (complete cancellation)
        self.con.execute("""
            DELETE FROM instrument_history WHERE isin = ?
        """, [isin])
        
        return {"status": "cancelled", "message": f"CANCELLED record for {isin}"}
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get delta processing statistics."""
        self.connection._ensure_connection()
        
        stats = {}
        
        # Version distribution
        version_stats = self.con.execute("""
            SELECT version_number, COUNT(*) as count
            FROM instruments
            GROUP BY version_number
            ORDER BY version_number
        """).fetchall()
        
        stats["version_distribution"] = {v: c for v, c in version_stats}
        
        # Record type distribution (history)
        if self._table_exists("instrument_history"):
            record_type_stats = self.con.execute("""
                SELECT record_type, COUNT(*) as count
                FROM instrument_history
                GROUP BY record_type
                ORDER BY count DESC
            """).fetchall()
            
            stats["historical_record_types"] = {rt: c for rt, c in record_type_stats}
        
        # Cancellations
        if self._table_exists("cancellations"):
            cancellation_count = self.con.execute("""
                SELECT COUNT(*) FROM cancellations
            """).fetchone()
            
            stats["total_cancellations"] = cancellation_count[0] if cancellation_count else 0
        
        return stats
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        result = self.con.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, [table_name]).fetchone()
        
        return result is not None
    
    def validate_version_integrity(self) -> Dict[str, Any]:
        """
        Validate version integrity across instruments and history tables.
        
        Returns:
            Dict with validation results and any issues found
        """
        self.connection._ensure_connection()
        
        issues = []
        stats = {}
        
        # Check for duplicate latest_record_flag
        duplicate_latest = self.con.execute("""
            SELECT isin, COUNT(*) as count
            FROM instruments
            WHERE latest_record_flag = TRUE
            GROUP BY isin
            HAVING COUNT(*) > 1
        """).fetchall()
        
        if duplicate_latest:
            issues.append(f"Found {len(duplicate_latest)} ISINs with multiple latest_record_flag=TRUE")
            stats["duplicate_latest_flags"] = duplicate_latest
        
        # Check for version gaps
        version_gaps = self.con.execute("""
            WITH version_check AS (
                SELECT isin, version_number,
                       LAG(version_number) OVER (PARTITION BY isin ORDER BY version_number) as prev_version
                FROM (
                    SELECT DISTINCT isin, version_number FROM instruments
                    UNION
                    SELECT DISTINCT isin, version_number FROM instrument_history
                ) all_versions
            )
            SELECT isin, version_number, prev_version
            FROM version_check
            WHERE prev_version IS NOT NULL 
              AND version_number != prev_version + 1
        """).fetchall()
        
        if version_gaps:
            issues.append(f"Found {len(version_gaps)} version gaps")
            stats["version_gaps"] = version_gaps
        
        # Check for overlapping validity periods
        overlapping_periods = self.con.execute("""
            SELECT h1.isin, h1.version_number, h1.valid_from_date, h1.valid_to_date,
                   h2.version_number as next_version, h2.valid_from_date as next_valid_from
            FROM instrument_history h1
            JOIN instrument_history h2 ON h1.isin = h2.isin 
              AND h1.version_number = h2.version_number - 1
            WHERE h1.valid_to_date >= h2.valid_from_date
        """).fetchall()
        
        if overlapping_periods:
            issues.append(f"Found {len(overlapping_periods)} overlapping validity periods")
            stats["overlapping_periods"] = overlapping_periods
        
        return {
            "status": "valid" if not issues else "issues_found",
            "issues": issues,
            "stats": stats,
            "validation_timestamp": datetime.now().isoformat()
        }