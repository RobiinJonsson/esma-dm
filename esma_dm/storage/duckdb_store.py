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
        
        self.con = duckdb.connect(self.db_path)
        initialize_schema(self.con)
        
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
    
    def index_all_csv_files(self, csv_dir: Path, pattern: str = "*.csv") -> int:
        """Index all CSV files in directory."""
        total = 0
        for csv_file in sorted(csv_dir.glob(pattern)):
            count = self.index_csv_file(csv_file)
            total += count
        return total
    
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
