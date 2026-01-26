"""
DuckDB bulk operations module.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np

from .connection import DuckDBConnection
from ..bulk import BulkInserter
from esma_dm.models.utils import CFI


class DuckDBOperations:
    """Handles bulk insert, update, and data processing operations."""
    
    def __init__(self, connection: DuckDBConnection):
        """Initialize operations with database connection."""
        self.connection = connection
        self.logger = logging.getLogger(__name__)
    
    @property
    def con(self):
        """Get database connection."""
        self.connection._ensure_connection()
        return self.connection.con
    
    def update(self, asset_type: Optional[str] = None):
        """
        Update database from cached CSV files.
        
        Args:
            asset_type: Filter by asset type (E, D, O, etc.). If None, processes all.
        
        Returns:
            Dict with update statistics
        """
        start_time = time.time()
        self.connection._ensure_connection()
        
        # Find CSV files in cache directory
        csv_dir = self.connection.cache_dir
        
        if asset_type:
            # Filter for specific asset type files
            pattern = f"*{asset_type}_*.csv"
            self.logger.info(f"Updating from {asset_type} asset type files")
        else:
            pattern = "*.csv"
            self.logger.info("Updating from all CSV files")
        
        csv_files = list(csv_dir.glob(pattern))
        
        if not csv_files:
            self.logger.warning(f"No CSV files found in {csv_dir} matching pattern {pattern}")
            return {
                "status": "no_files",
                "files_processed": 0,
                "total_instruments": 0,
                "duration_seconds": 0
            }
        
        total_instruments = 0
        files_processed = 0
        
        for csv_file in sorted(csv_files):
            try:
                instruments_count = self.index_csv_file(csv_file)
                total_instruments += instruments_count
                files_processed += 1
                self.logger.info(f"Processed {csv_file.name}: {instruments_count} instruments")
            except Exception as e:
                self.logger.error(f"Failed to process {csv_file}: {e}")
                continue
        
        duration = time.time() - start_time
        self.logger.info(f"Update completed: {files_processed} files, {total_instruments} instruments in {duration:.2f}s")
        
        return {
            "status": "completed",
            "files_processed": files_processed,
            "total_instruments": total_instruments,
            "duration_seconds": duration
        }
    
    def _find_column(self, df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
        """Find first matching column from a list of patterns."""
        for pattern in patterns:
            if pattern in df.columns:
                return pattern
        return None
    
    def _prepare_master_records(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Prepare master instrument records for bulk insert."""
        # Map various column name patterns (FIRDS uses 'Id' for ISIN)
        isin_col = self._find_column(df, ['Id', 'Isin', 'ISIN', 'isin'])
        cfi_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ClssfctnTp', 'CfiCd', 'CFI', 'cfi_code', 'CfiCode'])
        name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_FullNm', 'FullNm', 'FullName', 'full_name', 'Name'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm', 'ShrtNm', 'ShortName', 'short_name'])
        issuer_col = self._find_column(df, ['RefData_Issr', 'Issr', 'Issuer', 'issuer'])
        
        # Asset-specific fields from actual FIRDS data
        # Equity fields
        underlying_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN'])
        commodity_deriv_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_CmmdtyDerivInd'])
        
        # Debt fields  
        total_amount_col = self._find_column(df, ['RefData_DebtInstrmAttrbts_TtlIssdNmnlAmt'])
        maturity_col = self._find_column(df, ['RefData_DebtInstrmAttrbts_MtrtyDt'])
        nominal_value_col = self._find_column(df, ['RefData_DebtInstrmAttrbts_NmnlValPerUnit'])
        fixed_rate_col = self._find_column(df, ['RefData_DebtInstrmAttrbts_IntrstRate_Fxd'])
        debt_seniority_col = self._find_column(df, ['RefData_DebtInstrmAttrbts_DebtSnrty'])
        
        # Derivative fields (swaps, options, etc.)
        price_multiplier_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_PricMltplr'])
        delivery_type_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_DlvryTp'])
        expiry_date_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_XpryDt'])
        
        # Option-specific fields
        option_type_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_OptnTp'])
        option_exercise_style_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_OptnExrcStyle'])
        underlying_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN'])
        underlying_index_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_ISIN'])
        underlying_index_name_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Nm'])
        strike_price_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Amt'])
        strike_price_percentage_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_StrkPric_Pric_Pctg'])
        strike_price_basis_points_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_StrkPric_Pric_BsisPts'])
        
        # Additional derivative fields for futures/forwards/rights
        underlying_basket_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm_Bskt_ISIN'])
        fx_type_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_FxTp'])
        fx_other_notional_currency_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_OthrNtnlCcy'])
        commodity_base_product_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Nrgy_Elctrcty_BasePdct',
                                                              'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Nrgy_NtrlGas_BasePdct',
                                                              'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Metl_Prcs_BasePdct',
                                                              'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Agrcltrl_GrnOilSeed_BasePdct'])
        commodity_sub_product_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Nrgy_Elctrcty_SubPdct',
                                                            'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Nrgy_NtrlGas_SubPdct',
                                                            'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Metl_Prcs_SubPdct',
                                                            'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Agrcltrl_GrnOilSeed_SubPdct'])
        commodity_additional_sub_product_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Nrgy_Elctrcty_AddtlSubPdct',
                                                                       'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Metl_Prcs_AddtlSubPdct',
                                                                       'RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_Agrcltrl_GrnOilSeed_AddtlSubPdct'])
        
        if not isin_col:
            raise ValueError("ISIN column not found")
        
        # Create master records
        master_df = pd.DataFrame()
        master_df['isin'] = df[isin_col]
        master_df['cfi_code'] = df[cfi_col] if cfi_col else None
        master_df['full_name'] = df[name_col] if name_col else None
        master_df['short_name'] = df[short_name_col] if short_name_col else None
        master_df['issuer'] = df[issuer_col] if issuer_col else None
        master_df['source_file'] = source_file
        master_df['indexed_at'] = pd.Timestamp.now()
        
        # Add asset-specific fields
        # Equity fields
        master_df['underlying_instrument'] = df[underlying_col] if underlying_col else None
        master_df['commodity_derivative_indicator'] = df[commodity_deriv_col] if commodity_deriv_col else None
        
        # Debt fields
        master_df['total_issued_nominal_amount'] = df[total_amount_col] if total_amount_col else None
        master_df['maturity_date'] = df[maturity_col] if maturity_col else None
        master_df['nominal_value_per_unit'] = df[nominal_value_col] if nominal_value_col else None
        master_df['fixed_interest_rate'] = df[fixed_rate_col] if fixed_rate_col else None
        master_df['debt_seniority'] = df[debt_seniority_col] if debt_seniority_col else None
        
        # Derivative fields
        master_df['price_multiplier'] = df[price_multiplier_col] if price_multiplier_col else None
        master_df['delivery_type'] = df[delivery_type_col] if delivery_type_col else None
        master_df['expiry_date'] = df[expiry_date_col] if expiry_date_col else None
        
        # Option-specific fields
        master_df['option_type'] = df[option_type_col] if option_type_col else None
        master_df['option_exercise_style'] = df[option_exercise_style_col] if option_exercise_style_col else None
        master_df['underlying_isin'] = df[underlying_isin_col] if underlying_isin_col else None
        master_df['underlying_index_isin'] = df[underlying_index_isin_col] if underlying_index_isin_col else None
        master_df['underlying_index_name'] = df[underlying_index_name_col] if underlying_index_name_col else None
        master_df['strike_price'] = pd.to_numeric(df[strike_price_col], errors='coerce') if strike_price_col else None
        master_df['strike_price_percentage'] = pd.to_numeric(df[strike_price_percentage_col], errors='coerce') if strike_price_percentage_col else None
        master_df['strike_price_basis_points'] = pd.to_numeric(df[strike_price_basis_points_col], errors='coerce') if strike_price_basis_points_col else None
        
        # Additional derivative fields for futures/forwards/rights
        master_df['underlying_basket_isin'] = df[underlying_basket_isin_col] if underlying_basket_isin_col else None
        master_df['fx_type'] = df[fx_type_col] if fx_type_col else None
        master_df['fx_other_notional_currency'] = df[fx_other_notional_currency_col] if fx_other_notional_currency_col else None
        master_df['commodity_base_product'] = df[commodity_base_product_col] if commodity_base_product_col else None
        master_df['commodity_sub_product'] = df[commodity_sub_product_col] if commodity_sub_product_col else None
        master_df['commodity_additional_sub_product'] = df[commodity_additional_sub_product_col] if commodity_additional_sub_product_col else None
        
        # Add asset type based on CFI
        if cfi_col:
            master_df['asset_type'] = df[cfi_col].str[0] if cfi_col else None
        
        return master_df.dropna(subset=['isin'])
    
    def _insert_listings(self, df: pd.DataFrame, source_file: str):
        """Insert market listings if trading venue columns are present."""
        self.logger.debug(f"_insert_listings called for {source_file} with {len(df)} rows")
        print(f"[DEBUG] _insert_listings: {len(df)} rows from {source_file}")
        print(f"[DEBUG] Available columns: {list(df.columns)[:10]}...")  # Show first 10 columns
        
        # Map listing columns using actual FIRDS CSV column names
        isin_col = self._find_column(df, ['Id', 'ISIN', 'isin'])
        venue_col = self._find_column(df, [
            'RefData_TradgVnRltdAttrbts_Id',
            'TradgVnIssr', 
            'TradingVenue', 
            'trading_venue'
        ])
        
        print(f"[DEBUG] Found columns - ISIN: {isin_col}, Venue: {venue_col}")
        
        if not all([isin_col, venue_col]):
            self.logger.debug(f"Missing required columns for listings - ISIN: {isin_col}, Venue: {venue_col}")
            print(f"[DEBUG] Skipping listings - missing required columns")
            return
        
        # Map additional listing columns
        first_trade_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_FrstTradDt'])
        termination_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_TermntnDt'])
        admission_approval_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_AdmssnApprvlDtByIssr'])
        admission_request_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_ReqForAdmssnDt'])
        issuer_request_col = self._find_column(df, ['RefData_TradgVnRltdAttrbts_IssrReq'])
        
        # Prepare listings data
        listings_df = pd.DataFrame()
        listings_df['isin'] = df[isin_col]
        listings_df['trading_venue_id'] = df[venue_col]
        listings_df['first_trade_date'] = df[first_trade_col] if first_trade_col else None
        listings_df['termination_date'] = df[termination_col] if termination_col else None  
        listings_df['admission_approval_date'] = df[admission_approval_col] if admission_approval_col else None
        listings_df['request_for_admission_date'] = df[admission_request_col] if admission_request_col else None
        listings_df['issuer_request'] = df[issuer_request_col] if issuer_request_col else None
        listings_df['source_file'] = source_file
        listings_df['indexed_at'] = pd.Timestamp.now()
        
        # Remove rows with null required values
        listings_df = listings_df.dropna(subset=['isin', 'trading_venue_id'])
        
        print(f"[DEBUG] After dropna: {len(listings_df)} listings remaining")
        
        if len(listings_df) == 0:
            self.logger.debug("No valid listings data to insert")
            print(f"[DEBUG] No valid listings data after dropna")
            return
        
        # Insert into listings table
        try:
            # Register DataFrame with DuckDB
            self.con.register('listings_df', listings_df)
            
            # Use DuckDB's native bulk insert (id column is auto-increment, skip it)
            insert_query = """
                INSERT INTO listings 
                (isin, trading_venue_id, first_trade_date, termination_date, 
                 admission_approval_date, request_for_admission_date, issuer_request,
                 source_file, indexed_at)
                SELECT isin, trading_venue_id, first_trade_date, termination_date,
                       admission_approval_date, request_for_admission_date, issuer_request,
                       source_file, indexed_at
                FROM listings_df
            """
            
            result = self.con.execute(insert_query)
            print(f"[DEBUG] INSERT executed, rows affected: {len(listings_df)}")
            self.logger.info(f"Inserted {len(listings_df)} market listings")
            
        except Exception as e:
            self.logger.error(f"Failed to insert listings: {e}")
            raise
    
    def index_csv_file(self, csv_path: Path) -> int:
        """
        Index a single CSV file into the database.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Number of instruments processed
            
        Raises:
            Exception: If file processing fails
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        start_time = time.time()
        source_file = csv_path.name
        
        self.logger.info(f"Starting to index {source_file}")
        
        try:
            # Load CSV with error handling for different encodings
            try:
                df = pd.read_csv(csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                self.logger.warning(f"UTF-8 decode failed for {csv_path}, trying ISO-8859-1")
                df = pd.read_csv(csv_path, encoding='iso-8859-1')
            
            if len(df) == 0:
                self.logger.warning(f"Empty CSV file: {csv_path}")
                return 0
            
            self.logger.info(f"Loaded {len(df)} records from {source_file}")
            
            # Prepare master records
            master_df = self._prepare_master_records(df, source_file)
            
            if len(master_df) == 0:
                self.logger.warning(f"No valid instruments found in {source_file}")
                return 0
            
            # Group by asset type for bulk processing
            instruments_by_type = master_df.groupby('asset_type') if 'asset_type' in master_df.columns else {'unknown': master_df}
            
            self.con.execute("BEGIN TRANSACTION")
            
            total_inserted = 0
            
            try:
                # Use BulkInserter for efficient batch processing
                inserter = BulkInserter(self.con, self._find_column)
                
                # FIRST: Insert into main instruments table
                self.logger.debug(f"Inserting {len(master_df)} instruments into main table")
                inserter.insert_instruments(master_df)
                
                # THEN: Process each asset type separately for asset-specific tables
                for asset_type, asset_df in instruments_by_type:
                    if pd.isna(asset_type):
                        asset_type = 'unknown'
                    
                    self.logger.debug(f"Processing {len(asset_df)} {asset_type} instruments for asset-specific table")
                    
                    # Use appropriate insert method based on asset type
                    if asset_type == 'E':
                        inserter.insert_equities(asset_df)
                    elif asset_type == 'D':
                        inserter.insert_debt(asset_df)
                    elif asset_type == 'S':
                        inserter.insert_swaps(asset_df)
                    elif asset_type == 'F':
                        inserter.insert_futures(asset_df)
                    elif asset_type == 'O':  # Options
                        inserter.insert_options(asset_df)
                    elif asset_type == 'J':
                        inserter.insert_forwards(asset_df)
                    elif asset_type == 'R':  # Rights/Entitlements
                        inserter.insert_rights(asset_df)
                    elif asset_type == 'C':
                        inserter.insert_civs(asset_df)
                    elif asset_type == 'I':  # Spot Commodities
                        inserter.insert_spots(asset_df)
                    elif asset_type == 'H':  # Non-standard/Others
                        self.logger.warning(f"Non-standard asset type H found, skipping {len(asset_df)} instruments")
                        continue
                    else:
                        self.logger.warning(f"Unknown asset type: {asset_type}, skipping")
                        continue
                    
                    inserted_count = len(asset_df)
                    total_inserted += inserted_count
                    self.logger.debug(f"Processed {inserted_count} {asset_type} instruments")
                
                # Insert market listings if data is available
                self._insert_listings(df, source_file)
                
                self.con.execute("COMMIT")
                
                duration = time.time() - start_time
                rate = total_inserted / duration if duration > 0 else 0
                
                self.logger.info(
                    f"Successfully indexed {source_file}: {total_inserted} instruments "
                    f"in {duration:.2f}s ({rate:.0f} instruments/sec)"
                )
                
                return total_inserted
                
            except Exception as e:
                self.con.execute("ROLLBACK")
                self.logger.error(f"Transaction failed for {source_file}: {e}")
                raise
            
        except Exception as e:
            self.logger.error(f"Failed to index {csv_path}: {e}")
            raise
    
    def index_all_csv_files(self, csv_dir: Path, pattern: str = "*.csv", delete_csv: bool = False) -> dict:
        """
        Index all CSV files in a directory.
        
        Args:
            csv_dir: Directory containing CSV files
            pattern: File pattern to match (default: "*.csv")
            delete_csv: Whether to delete CSV files after processing
            
        Returns:
            Dict with processing statistics
        """
        if not csv_dir.exists():
            raise FileNotFoundError(f"Directory not found: {csv_dir}")
        
        csv_files = list(csv_dir.glob(pattern))
        
        if not csv_files:
            self.logger.warning(f"No files matching pattern '{pattern}' found in {csv_dir}")
            return {"files_processed": 0, "total_instruments": 0}
        
        start_time = time.time()
        total_instruments = 0
        files_processed = 0
        failed_files = []
        
        for csv_file in sorted(csv_files):
            try:
                instruments_count = self.index_csv_file(csv_file)
                total_instruments += instruments_count
                files_processed += 1
                
                # Delete file if requested and processing succeeded
                if delete_csv:
                    csv_file.unlink()
                    self.logger.debug(f"Deleted processed file: {csv_file}")
                    
            except Exception as e:
                failed_files.append({"file": csv_file.name, "error": str(e)})
                self.logger.error(f"Failed to process {csv_file}: {e}")
        
        duration = time.time() - start_time
        
        result = {
            "files_processed": files_processed,
            "total_instruments": total_instruments,
            "failed_files": failed_files,
            "duration_seconds": duration
        }
        
        self.logger.info(
            f"Batch processing completed: {files_processed} files, "
            f"{total_instruments} instruments in {duration:.2f}s"
        )
        
        return result