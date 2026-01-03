"""
DuckDB storage backend with vectorized bulk loading.

Architecture:
- Master table (instruments): Core fields for all instruments
- Asset-specific tables: equity_instruments, debt_instruments, derivative_instruments, other_instruments
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


class DuckDBStorage(StorageBackend):
    """
    DuckDB storage with star schema and vectorized bulk loading.
    
    Schema:
    - instruments (master/index table): ISIN, CFI, type, issuer, name
    - equity_instruments: Equity-specific fields
    - debt_instruments: Debt-specific fields  
    - derivative_instruments: Derivative-specific fields
    - other_instruments: Catch-all for other types
    
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
        self._init_schema()
        
    def _init_schema(self):
        """Create database schema with master + asset-specific tables."""
        
        # Master/index table - core fields only
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS instruments (
                isin VARCHAR PRIMARY KEY,
                cfi_code VARCHAR,
                instrument_type VARCHAR,
                issuer VARCHAR,
                full_name VARCHAR,
                currency VARCHAR,
                source_file VARCHAR,
                indexed_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT now()
            )
        """)
        
        # Equity instruments - detailed schema
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS equity_instruments (
                isin VARCHAR PRIMARY KEY,
                short_name VARCHAR,
                dividend_payment_frequency VARCHAR,
                voting_rights_per_share VARCHAR,
                ownership_restriction VARCHAR,
                redemption_type VARCHAR,
                capital_investment_restriction VARCHAR,
                trading_venue_id VARCHAR,
                first_trade_date DATE,
                termination_date DATE,
                FOREIGN KEY (isin) REFERENCES instruments(isin)
            )
        """)
        
        # Debt instruments - detailed schema  
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS debt_instruments (
                isin VARCHAR PRIMARY KEY,
                short_name VARCHAR,
                maturity_date DATE,
                total_issued_nominal_amount DOUBLE,
                nominal_value_per_unit DOUBLE,
                interest_rate_type VARCHAR,
                fixed_rate DOUBLE,
                floating_rate_reference VARCHAR,
                debt_seniority VARCHAR,
                trading_venue_id VARCHAR,
                first_trade_date DATE,
                termination_date DATE,
                FOREIGN KEY (isin) REFERENCES instruments(isin)
            )
        """)
        
        # Derivative instruments - detailed schema
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS derivative_instruments (
                isin VARCHAR PRIMARY KEY,
                short_name VARCHAR,
                expiry_date DATE,
                price_multiplier DOUBLE,
                underlying_isin VARCHAR,
                underlying_index_name VARCHAR,
                base_product VARCHAR,
                sub_product VARCHAR,
                option_type VARCHAR,
                strike_price DOUBLE,
                strike_price_currency VARCHAR,
                delivery_type VARCHAR,
                trading_venue_id VARCHAR,
                first_trade_date DATE,
                termination_date DATE,
                FOREIGN KEY (isin) REFERENCES instruments(isin)
            )
        """)
        
        # Other instruments (catch-all for C, O, R, etc.)
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS other_instruments (
                isin VARCHAR PRIMARY KEY,
                short_name VARCHAR,
                trading_venue_id VARCHAR,
                first_trade_date DATE,
                termination_date DATE,
                FOREIGN KEY (isin) REFERENCES instruments(isin)
            )
        """)
        
        # Metadata table
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                file_name VARCHAR PRIMARY KEY,
                indexed_at TIMESTAMP,
                instruments_count INTEGER,
                file_type VARCHAR,
                file_date DATE
            )
        """)
        
        # Create indexes
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_instruments_type ON instruments(instrument_type)")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_instruments_cfi ON instruments(cfi_code)")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_equity_venue ON equity_instruments(trading_venue_id)")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_debt_venue ON debt_instruments(trading_venue_id)")
        self.con.execute("CREATE INDEX IF NOT EXISTS idx_deriv_venue ON derivative_instruments(trading_venue_id)")
    
    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """Find first matching column from patterns."""
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in col.lower():
                    return col
        return None
    
    def _prepare_master_records(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """
        Prepare master/index records from raw CSV DataFrame.
        
        Returns DataFrame with columns: isin, cfi_code, instrument_type, issuer, full_name, currency, source_file, indexed_at
        """
        # Find ISIN column
        isin_col = self._find_column(df, ['Id', 'ISIN', 'isin'])
        
        # Find CFI code column
        cfi_col = self._find_column(df, ['ClssfctnTp', 'cfi_code', 'classification_type'])
        
        # Find issuer column  
        issuer_col = self._find_column(df, ['Issr', 'issuer', 'LEI', 'lei'])
        
        # Find name columns
        full_name_col = self._find_column(df, ['FullNm', 'full_name', 'name'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        
        # Find currency column
        currency_col = self._find_column(df, ['NtnlCcy', 'currency', 'notional_currency'])
        
        master = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'cfi_code': df[cfi_col] if cfi_col else None,
            'instrument_type': df[cfi_col].str[0] if cfi_col else 'O',  # First letter
            'issuer': df[issuer_col] if issuer_col else None,
            'full_name': df[full_name_col] if full_name_col else (df[short_name_col] if short_name_col else None),
            'currency': df[currency_col] if currency_col else None,
            'source_file': file_name,
            'indexed_at': datetime.now()
        })
        
        # Drop rows without ISIN
        master = master.dropna(subset=['isin'])
        
        return master
    
    def _bulk_insert_equities(self, df: pd.DataFrame):
        """Bulk insert equity instruments."""
        # Find relevant columns
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        div_freq_col = self._find_column(df, ['DvddPmtFrqcy', 'dividend_frequency'])
        voting_col = self._find_column(df, ['VtngRghtsPerShr', 'voting_rights'])
        ownership_col = self._find_column(df, ['OwnrshpRstrctn', 'ownership_restriction'])
        redemption_col = self._find_column(df, ['RdmptnTp', 'redemption_type'])
        capital_col = self._find_column(df, ['CptlInvstmntRstrctn', 'capital_restriction'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        equity_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'dividend_payment_frequency': df[div_freq_col] if div_freq_col else None,
            'voting_rights_per_share': df[voting_col] if voting_col else None,
            'ownership_restriction': df[ownership_col] if ownership_col else None,
            'redemption_type': df[redemption_col] if redemption_col else None,
            'capital_investment_restriction': df[capital_col] if capital_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
        })
        
        equity_df = equity_df.dropna(subset=['isin'])
        
        if len(equity_df) > 0:
            self.con.execute("""
                INSERT INTO equity_instruments 
                SELECT * FROM equity_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    dividend_payment_frequency = EXCLUDED.dividend_payment_frequency,
                    voting_rights_per_share = EXCLUDED.voting_rights_per_share,
                    ownership_restriction = EXCLUDED.ownership_restriction,
                    redemption_type = EXCLUDED.redemption_type,
                    capital_investment_restriction = EXCLUDED.capital_investment_restriction,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def _bulk_insert_debt(self, df: pd.DataFrame):
        """Bulk insert debt instruments."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        maturity_col = self._find_column(df, ['MtrtyDt', 'maturity_date'])
        total_issued_col = self._find_column(df, ['TtlIssdNmnlAmt', 'total_issued_nominal'])
        nominal_col = self._find_column(df, ['MnmNmnlQt', 'nominal_value'])
        rate_type_col = self._find_column(df, ['IntrstRateTp', 'interest_rate_type'])
        fixed_rate_col = self._find_column(df, ['FxdIntrstRate', 'fixed_rate'])
        float_ref_col = self._find_column(df, ['FltgIntrstRateRef', 'floating_rate_reference'])
        seniority_col = self._find_column(df, ['DebtSnrty', 'debt_seniority'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        debt_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'maturity_date': pd.to_datetime(df[maturity_col], errors='coerce') if maturity_col else None,
            'total_issued_nominal_amount': pd.to_numeric(df[total_issued_col], errors='coerce') if total_issued_col else None,
            'nominal_value_per_unit': pd.to_numeric(df[nominal_col], errors='coerce') if nominal_col else None,
            'interest_rate_type': df[rate_type_col] if rate_type_col else None,
            'fixed_rate': pd.to_numeric(df[fixed_rate_col], errors='coerce') if fixed_rate_col else None,
            'floating_rate_reference': df[float_ref_col] if float_ref_col else None,
            'debt_seniority': df[seniority_col] if seniority_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
        })
        
        debt_df = debt_df.dropna(subset=['isin'])
        
        if len(debt_df) > 0:
            self.con.execute("""
                INSERT INTO debt_instruments 
                SELECT * FROM debt_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    maturity_date = EXCLUDED.maturity_date,
                    total_issued_nominal_amount = EXCLUDED.total_issued_nominal_amount,
                    nominal_value_per_unit = EXCLUDED.nominal_value_per_unit,
                    interest_rate_type = EXCLUDED.interest_rate_type,
                    fixed_rate = EXCLUDED.fixed_rate,
                    floating_rate_reference = EXCLUDED.floating_rate_reference,
                    debt_seniority = EXCLUDED.debt_seniority,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def _bulk_insert_derivatives(self, df: pd.DataFrame):
        """Bulk insert derivative instruments."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        expiry_col = self._find_column(df, ['XpryDt', 'expiry_date'])
        multiplier_col = self._find_column(df, ['PricMltplr', 'price_multiplier'])
        underlying_isin_col = self._find_column(df, ['UndrlygISIN', 'underlying_isin'])
        underlying_index_col = self._find_column(df, ['UndrlygIndxNm', 'underlying_index'])
        base_product_col = self._find_column(df, ['DerivBasePdct', 'base_product'])
        sub_product_col = self._find_column(df, ['DerivSubPdct', 'sub_product'])
        option_type_col = self._find_column(df, ['OptnTp', 'option_type'])
        strike_price_col = self._find_column(df, ['StrkPric', 'strike_price'])
        strike_currency_col = self._find_column(df, ['StrkPricCcy', 'strike_currency'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        deriv_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else None,
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'underlying_index_name': df[underlying_index_col] if underlying_index_col else None,
            'base_product': df[base_product_col] if base_product_col else None,
            'sub_product': df[sub_product_col] if sub_product_col else None,
            'option_type': df[option_type_col] if option_type_col else None,
            'strike_price': pd.to_numeric(df[strike_price_col], errors='coerce') if strike_price_col else None,
            'strike_price_currency': df[strike_currency_col] if strike_currency_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
        })
        
        deriv_df = deriv_df.dropna(subset=['isin'])
        
        if len(deriv_df) > 0:
            self.con.execute("""
                INSERT INTO derivative_instruments 
                SELECT * FROM deriv_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    expiry_date = EXCLUDED.expiry_date,
                    price_multiplier = EXCLUDED.price_multiplier,
                    underlying_isin = EXCLUDED.underlying_isin,
                    underlying_index_name = EXCLUDED.underlying_index_name,
                    base_product = EXCLUDED.base_product,
                    sub_product = EXCLUDED.sub_product,
                    option_type = EXCLUDED.option_type,
                    strike_price = EXCLUDED.strike_price,
                    strike_price_currency = EXCLUDED.strike_price_currency,
                    delivery_type = EXCLUDED.delivery_type,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def _bulk_insert_others(self, df: pd.DataFrame):
        """Bulk insert other instruments."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        other_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
        })
        
        other_df = other_df.dropna(subset=['isin'])
        
        if len(other_df) > 0:
            self.con.execute("""
                INSERT INTO other_instruments 
                SELECT * FROM other_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def index_csv_file(self, file_path: str) -> int:
        """
        Index a FIRDS CSV file using vectorized bulk loading.
        
        Strategy:
        1. Load entire CSV into DataFrame
        2. Prepare master records (all instruments)
        3. Bulk insert master records
        4. Group by asset type
        5. Bulk insert type-specific records
        
        Returns:
            Number of instruments indexed
        """
        start_time = time.time()
        file_name = Path(file_path).name
        
        # Check if already indexed
        result = self.con.execute(
            "SELECT COUNT(*) as count FROM metadata WHERE file_name = ?",
            [file_name]
        ).fetchone()
        
        if result and result[0] > 0:
            self.logger.info(f"File {file_name} already indexed, skipping")
            return 0
        
        self.logger.info(f"Loading CSV file: {file_name}")
        
        # Load entire CSV - let pandas infer types
        df = pd.read_csv(file_path, low_memory=False)
        
        self.logger.info(f"Loaded {len(df)} rows, preparing master records...")
        
        # Prepare master records (all instruments)
        master_df = self._prepare_master_records(df, file_name)
        
        # Bulk insert master records
        self.logger.info(f"Inserting {len(master_df)} master records...")
        self.con.execute("""
            INSERT INTO instruments (isin, cfi_code, instrument_type, issuer, full_name, currency, source_file, indexed_at)
            SELECT * FROM master_df
            ON CONFLICT (isin) DO UPDATE SET
                cfi_code = EXCLUDED.cfi_code,
                instrument_type = EXCLUDED.instrument_type,
                issuer = EXCLUDED.issuer,
                full_name = EXCLUDED.full_name,
                currency = EXCLUDED.currency,
                source_file = EXCLUDED.source_file,
                updated_at = now()
        """)
        
        # Extract asset type from CFI code (first letter)
        cfi_col = self._find_column(df, ['ClssfctnTp', 'cfi_code', 'classification_type'])
        
        if cfi_col:
            df['_asset_type'] = df[cfi_col].str[0].fillna('O')
        else:
            df['_asset_type'] = 'O'  # Other/unknown
        
        # Group by asset type and insert into specific tables
        asset_groups = df.groupby('_asset_type')
        
        for asset_type, group_df in asset_groups:
            count = len(group_df)
            self.logger.info(f"Processing {count} {asset_type} instruments...")
            
            if asset_type == 'E':
                self._bulk_insert_equities(group_df)
            elif asset_type == 'D':
                self._bulk_insert_debt(group_df)
            elif asset_type in ['F', 'O', 'S']:  # Derivatives (Futures, Options, Swaps)
                self._bulk_insert_derivatives(group_df)
            else:
                self._bulk_insert_others(group_df)
        
        # Update metadata
        file_type = 'DLTINS' if 'DLTINS' in file_name else 'FULINS'
        file_date = None  # Extract from filename if needed
        
        self.con.execute("""
            INSERT INTO metadata (file_name, indexed_at, instruments_count, file_type, file_date)
            VALUES (?, now(), ?, ?, ?)
        """, [file_name, len(master_df), file_type, file_date])
        
        elapsed = time.time() - start_time
        rate = len(master_df) / elapsed if elapsed > 0 else 0
        self.logger.info(f"Indexed {len(master_df)} instruments in {elapsed:.2f}s ({rate:.0f} instruments/second)")
        
        return len(master_df)
    
    def get_instrument(self, isin: str) -> Optional[Dict]:
        """
        Get instrument by ISIN.
        
        Queries master table, determines type, then joins with specific table.
        
        Returns:
            Dictionary with all instrument data
        """
        # Get master record
        master = self.con.execute("""
            SELECT isin, cfi_code, instrument_type, issuer, full_name, currency, source_file, indexed_at, updated_at
            FROM instruments
            WHERE isin = ?
        """, [isin]).fetchone()
        
        if not master:
            return None
        
        result = {
            'isin': master[0],
            'cfi_code': master[1],
            'instrument_type': master[2],
            'issuer': master[3],
            'full_name': master[4],
            'currency': master[5],
            'source_file': master[6],
            'indexed_at': master[7],
            'updated_at': master[8]
        }
        
        # Get type-specific data
        instrument_type = master[2]
        
        if instrument_type == 'E':
            details = self.con.execute("""
                SELECT * FROM equity_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if details:
                result.update({
                    'short_name': details[1],
                    'dividend_payment_frequency': details[2],
                    'voting_rights_per_share': details[3],
                    'ownership_restriction': details[4],
                    'redemption_type': details[5],
                    'capital_investment_restriction': details[6],
                    'trading_venue_id': details[7],
                    'first_trade_date': details[8],
                    'termination_date': details[9]
                })
        
        elif instrument_type == 'D':
            details = self.con.execute("""
                SELECT * FROM debt_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if details:
                result.update({
                    'short_name': details[1],
                    'maturity_date': details[2],
                    'total_issued_nominal_amount': details[3],
                    'nominal_value_per_unit': details[4],
                    'interest_rate_type': details[5],
                    'fixed_rate': details[6],
                    'floating_rate_reference': details[7],
                    'debt_seniority': details[8],
                    'trading_venue_id': details[9],
                    'first_trade_date': details[10],
                    'termination_date': details[11]
                })
        
        elif instrument_type in ['F', 'O', 'S']:
            details = self.con.execute("""
                SELECT * FROM derivative_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if details:
                result.update({
                    'short_name': details[1],
                    'expiry_date': details[2],
                    'price_multiplier': details[3],
                    'underlying_isin': details[4],
                    'underlying_index_name': details[5],
                    'base_product': details[6],
                    'sub_product': details[7],
                    'option_type': details[8],
                    'strike_price': details[9],
                    'strike_price_currency': details[10],
                    'delivery_type': details[11],
                    'trading_venue_id': details[12],
                    'first_trade_date': details[13],
                    'termination_date': details[14]
                })
        
        else:
            details = self.con.execute("""
                SELECT * FROM other_instruments WHERE isin = ?
            """, [isin]).fetchone()
            if details:
                result.update({
                    'short_name': details[1],
                    'trading_venue_id': details[2],
                    'first_trade_date': details[3],
                    'termination_date': details[4]
                })
        
        return result
    
    def get_stats_by_asset_type(self) -> pd.DataFrame:
        """Get statistics by asset type."""
        return self.con.execute("""
            SELECT 
                instrument_type as asset_type,
                CASE instrument_type
                    WHEN 'E' THEN 'Equities'
                    WHEN 'D' THEN 'Debt Instruments'
                    WHEN 'F' THEN 'Futures'
                    WHEN 'O' THEN 'Options'
                    WHEN 'S' THEN 'Swaps'
                    WHEN 'C' THEN 'Collective Investment Vehicles'
                    WHEN 'R' THEN 'Entitlements (Rights)'
                    WHEN 'H' THEN 'Referential Instruments'
                    WHEN 'I' THEN 'Indices'
                    WHEN 'J' THEN 'Listed Options'
                    ELSE 'Other'
                END as asset_name,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM instruments
            GROUP BY instrument_type
            ORDER BY count DESC
        """).fetchdf()
    
    def get_instrument_history(self, isin: str) -> List[Dict]:
        """Get historical states for an instrument (not implemented in vectorized version)."""
        return []
    
    def index_all_csv_files(self, cache_dir: Optional[Path] = None, delete_csv: bool = False) -> Dict:
        """Index all CSV files in directory."""
        if cache_dir is None:
            cache_dir = self.cache_dir
        
        csv_files = list(Path(cache_dir).rglob('*.csv'))
        results = {'total_files': len(csv_files), 'indexed': 0, 'skipped': 0, 'errors': 0}
        
        for csv_file in csv_files:
            try:
                count = self.index_csv_file(str(csv_file))
                if count > 0:
                    results['indexed'] += 1
                    if delete_csv:
                        csv_file.unlink()
                else:
                    results['skipped'] += 1
            except Exception as e:
                self.logger.error(f"Error indexing {csv_file}: {e}")
                results['errors'] += 1
        
        return results
    
    def search_instruments(self, **filters) -> List[Dict]:
        """Search instruments by filters."""
        where_clauses = []
        params = []
        
        for key, value in filters.items():
            where_clauses.append(f"{key} = ?")
            params.append(value)
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        results = self.con.execute(f"""
            SELECT isin FROM instruments WHERE {where_sql}
        """, params).fetchall()
        
        return [self.get_instrument(row[0]) for row in results]
    
    def get_stats(self) -> Dict:
        """Get storage statistics."""
        total = self.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        files = self.con.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
        
        return {
            'total_instruments': total,
            'files_processed': files,
            'database_path': self.db_path
        }
    
    def close(self):
        """Close database connection."""
        if self.con:
            self.con.close()
