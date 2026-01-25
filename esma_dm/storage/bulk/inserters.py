"""
Bulk insert handlers for each asset type.

Each handler extracts relevant fields from CSV DataFrame and performs bulk insert.
"""

import pandas as pd
from typing import Callable


class BulkInserter:
    """Handles bulk insertion of instruments by asset type."""
    
    def __init__(self, con, find_column_func: Callable):
        """
        Initialize bulk inserter.
        
        Args:
            con: DuckDB connection
            find_column_func: Function to find columns in DataFrame
        """
        self.con = con
        self._find_column = find_column_func
    
    def insert_instruments(self, df: pd.DataFrame):
        """Bulk insert into main instruments table."""
        print(f"[DEBUG] Inserting {len(df)} instruments into main table")
        print(f"[DEBUG] Available columns: {list(df.columns)}")
        
        # Map processed DataFrame columns to instruments table schema
        instruments_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'cfi_code': df['cfi_code'] if 'cfi_code' in df.columns else pd.Series([None] * len(df)),
            'instrument_type': df['asset_type'] if 'asset_type' in df.columns else pd.Series([None] * len(df)),
            'issuer': df['issuer'] if 'issuer' in df.columns else pd.Series([None] * len(df)),
            'full_name': df['full_name'] if 'full_name' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'currency': df['currency'] if 'currency' in df.columns else pd.Series([None] * len(df)),
            'valid_from_date': df['valid_from_date'] if 'valid_from_date' in df.columns else pd.Series([None] * len(df)),
            'valid_to_date': df['valid_to_date'] if 'valid_to_date' in df.columns else pd.Series([None] * len(df)),
            'latest_record_flag': pd.Series([True] * len(df)),
            'record_type': df['record_type'] if 'record_type' in df.columns else pd.Series(['FULINS'] * len(df)),
            'version_number': pd.Series([1] * len(df)),
            'source_file': df['source_file'] if 'source_file' in df.columns else pd.Series([None] * len(df)),
            'source_file_type': pd.Series(['FULINS'] * len(df)),
            'last_update_timestamp': pd.Series([None] * len(df)),
            'inconsistency_indicator': pd.Series([None] * len(df)),
            'indexed_at': df['indexed_at'] if 'indexed_at' in df.columns else pd.Series([None] * len(df)),
        })
        
        # Remove rows with missing ISIN
        instruments_df = instruments_df.dropna(subset=['isin'])
        
        if len(instruments_df) > 0:
            print(f"[DEBUG] Attempting to insert {len(instruments_df)} main instruments")
            try:
                # Use INSERT OR IGNORE to handle duplicates
                insert_query = """
                    INSERT OR IGNORE INTO instruments 
                    (isin, cfi_code, instrument_type, issuer, full_name, short_name, currency,
                     valid_from_date, valid_to_date, latest_record_flag, record_type,
                     version_number, source_file, source_file_type, last_update_timestamp,
                     inconsistency_indicator, indexed_at)
                    SELECT * FROM instruments_temp
                """
                
                # Create temporary view and insert
                self.con.execute("CREATE OR REPLACE VIEW instruments_temp AS SELECT * FROM instruments_df")
                self.con.execute(insert_query)
                
                print(f"[DEBUG] Successfully inserted main instruments")
            except Exception as e:
                print(f"[DEBUG] Main instruments INSERT failed with error: {e}")
                raise
        else:
            print(f"[DEBUG] No valid main instruments to insert")
    
    def insert_equities(self, df: pd.DataFrame):
        """Bulk insert equity instruments (E) using processed DataFrame structure."""
        # Add debugging
        print(f"[DEBUG] Processing equity DataFrame with shape: {df.shape}")
        print(f"[DEBUG] Available columns: {list(df.columns)}")
        
        # Only insert equity-specific fields (general fields go in main instruments table)
        # Based on actual FIRDS data structure
        equity_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_instrument': df['underlying_instrument'] if 'underlying_instrument' in df.columns else pd.Series([None] * len(df)),
            'commodity_derivative_indicator': df['commodity_derivative_indicator'] if 'commodity_derivative_indicator' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))  # Default version number for current mode
        })
        
        equity_df = equity_df.dropna(subset=['isin'])
        print(f"[DEBUG] Equity DataFrame after processing: {equity_df.shape}")
        print(f"[DEBUG] Sample equity data: {equity_df.head(2).to_dict('records') if len(equity_df) > 0 else 'No data'}")
        
        if len(equity_df) > 0:
            print(f"[DEBUG] Attempting to insert {len(equity_df)} equity instruments")
            try:
                self.con.execute("""
                    INSERT INTO equity_instruments 
                    SELECT * FROM equity_df
                    ON CONFLICT (isin) DO UPDATE SET
                        underlying_instrument = EXCLUDED.underlying_instrument,
                        commodity_derivative_indicator = EXCLUDED.commodity_derivative_indicator,
                        version_number = EXCLUDED.version_number
                """)
                print(f"[DEBUG] INSERT query executed successfully")
            except Exception as e:
                print(f"[DEBUG] INSERT failed with error: {e}")
                raise
        else:
            print(f"[DEBUG] No equity data to insert after filtering")
    
    def insert_debt(self, df: pd.DataFrame):
        """Bulk insert debt instruments (D) using actual FIRDS data structure."""
        print(f"[DEBUG] Processing debt DataFrame with shape: {df.shape}")
        print(f"[DEBUG] Available columns: {list(df.columns)}")
        
        # Only insert debt-specific fields based on actual FIRDS structure
        debt_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'total_issued_nominal_amount': df['total_issued_nominal_amount'] if 'total_issued_nominal_amount' in df.columns else pd.Series([None] * len(df)),
            'maturity_date': df['maturity_date'] if 'maturity_date' in df.columns else pd.Series([None] * len(df)),
            'nominal_value_per_unit': df['nominal_value_per_unit'] if 'nominal_value_per_unit' in df.columns else pd.Series([None] * len(df)),
            'fixed_interest_rate': df['fixed_interest_rate'] if 'fixed_interest_rate' in df.columns else pd.Series([None] * len(df)),
            'debt_seniority': df['debt_seniority'] if 'debt_seniority' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        debt_df = debt_df.dropna(subset=['isin'])
        print(f"[DEBUG] Debt DataFrame after processing: {debt_df.shape}")
        print(f"[DEBUG] Sample debt data: {debt_df.head(2).to_dict('records') if len(debt_df) > 0 else 'No data'}")
        
        if len(debt_df) > 0:
            print(f"[DEBUG] Attempting to insert {len(debt_df)} debt instruments")
            try:
                self.con.execute("""
                    INSERT INTO debt_instruments 
                    SELECT * FROM debt_df
                    ON CONFLICT (isin) DO UPDATE SET
                        total_issued_nominal_amount = EXCLUDED.total_issued_nominal_amount,
                        maturity_date = EXCLUDED.maturity_date,
                        nominal_value_per_unit = EXCLUDED.nominal_value_per_unit,
                        fixed_interest_rate = EXCLUDED.fixed_interest_rate,
                        debt_seniority = EXCLUDED.debt_seniority,
                        version_number = EXCLUDED.version_number
                """)
                print(f"[DEBUG] Debt INSERT query executed successfully")
            except Exception as e:
                print(f"[DEBUG] Debt INSERT failed with error: {e}")
                raise
        else:
            print(f"[DEBUG] No debt data to insert after filtering")

    def insert_swaps(self, df: pd.DataFrame):
        """Bulk insert swap instruments (S) using actual FIRDS data structure."""
        print(f"[DEBUG] Processing swap DataFrame with shape: {df.shape}")
        print(f"[DEBUG] Available columns: {list(df.columns)}")
        
        # Only insert swap-specific fields based on actual FIRDS structure
        swap_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'price_multiplier': df['price_multiplier'] if 'price_multiplier' in df.columns else pd.Series([None] * len(df)),
            'delivery_type': df['delivery_type'] if 'delivery_type' in df.columns else pd.Series([None] * len(df)),
            'expiry_date': df['expiry_date'] if 'expiry_date' in df.columns else pd.Series([None] * len(df)),
            'asset_class_specific': df['asset_class_specific'] if 'asset_class_specific' in df.columns else pd.Series([None] * len(df)),
            'underlying_instrument': df['underlying_instrument'] if 'underlying_instrument' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        swap_df = swap_df.dropna(subset=['isin'])
        print(f"[DEBUG] Swap DataFrame after processing: {swap_df.shape}")
        print(f"[DEBUG] Sample swap data: {swap_df.head(2).to_dict('records') if len(swap_df) > 0 else 'No data'}")
        
        if len(swap_df) > 0:
            print(f"[DEBUG] Attempting to insert {len(swap_df)} swap instruments")
            try:
                self.con.execute("""
                    INSERT INTO swap_instruments 
                    SELECT * FROM swap_df
                    ON CONFLICT (isin) DO UPDATE SET
                        price_multiplier = EXCLUDED.price_multiplier,
                        delivery_type = EXCLUDED.delivery_type,
                        expiry_date = EXCLUDED.expiry_date,
                        asset_class_specific = EXCLUDED.asset_class_specific,
                        underlying_instrument = EXCLUDED.underlying_instrument,
                        version_number = EXCLUDED.version_number
                """)
                print(f"[DEBUG] Swap INSERT query executed successfully")
            except Exception as e:
                print(f"[DEBUG] Swap INSERT failed with error: {e}")
                raise
        else:
            print(f"[DEBUG] No swap data to insert after filtering")
    
    def insert_futures(self, df: pd.DataFrame):
        """Bulk insert futures instruments (F) using actual FIRDS data structure."""
        # Use actual column names from population analysis
        isin_col = self._find_column(df, ['Id'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm'])
        expiry_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_XpryDt'])
        multiplier_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_PricMltplr'])
        delivery_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_DlvryTp'])
        underlying_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm'])
        competent_auth_col = self._find_column(df, ['RefData_TechAttrbts_RlvntCmptntAuthrty'])
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt'])
        
        futures_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else pd.Series([None] * len(df)),
            'short_name': df[short_name_col] if short_name_col else pd.Series([None] * len(df)),
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else pd.Series([None] * len(df)),
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else pd.Series([None] * len(df)),
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else pd.Series([None] * len(df)),
            'underlying_index_name': pd.Series([None] * len(df)),        # Not available in analyzed data
            'underlying_index_term_value': pd.Series([None] * len(df)),  # Not available in analyzed data
            'underlying_index_term_unit': pd.Series([None] * len(df)),   # Not available in analyzed data
            'notional_currency_1': pd.Series([None] * len(df)),          # Not available in analyzed data
            'notional_currency_2': pd.Series([None] * len(df)),          # Not available in analyzed data
            'delivery_type': df[delivery_col] if delivery_col else pd.Series([None] * len(df)),
            'competent_authority': df[competent_auth_col] if competent_auth_col else pd.Series([None] * len(df)),
            'publication_date': pd.to_datetime(df[publication_date_col], errors='coerce') if publication_date_col else pd.Series([None] * len(df))
        })
        
        futures_df = futures_df.dropna(subset=['isin'])
        
        if len(futures_df) > 0:
            self.con.execute("""
                INSERT INTO futures_instruments 
                SELECT * FROM futures_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    expiry_date = EXCLUDED.expiry_date,
                    price_multiplier = EXCLUDED.price_multiplier,
                    underlying_isin = EXCLUDED.underlying_isin,
                    underlying_index_name = EXCLUDED.underlying_index_name,
                    delivery_type = EXCLUDED.delivery_type,
                    commodity_base_product = EXCLUDED.commodity_base_product,
                    commodity_sub_product = EXCLUDED.commodity_sub_product,
                    commodity_additional_sub_product = EXCLUDED.commodity_additional_sub_product,
                    competent_authority = EXCLUDED.competent_authority,
                    publication_date = EXCLUDED.publication_date
            """)
    
    def insert_options(self, df: pd.DataFrame):
        """Bulk insert option instruments (O, H) using actual FIRDS data structure."""
        # Use actual column names from population analysis
        isin_col = self._find_column(df, ['Id'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm'])
        expiry_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_XpryDt'])
        multiplier_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_PricMltplr'])
        underlying_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm'])
        option_type_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_OptnTp'])
        exercise_style_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_OptnExrcStyle'])
        delivery_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_DlvryTp'])
        strike_price_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_StrkPric_Pric'])
        competent_auth_col = self._find_column(df, ['RefData_TechAttrbts_RlvntCmptntAuthrty'])
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt'])
        
        option_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else pd.Series([None] * len(df)),
            'short_name': df[short_name_col] if short_name_col else pd.Series([None] * len(df)),
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else pd.Series([None] * len(df)),
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else pd.Series([None] * len(df)),
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else pd.Series([None] * len(df)),
            'underlying_index_isin': pd.Series([None] * len(df)),         # Not available in analyzed data
            'underlying_index_name': pd.Series([None] * len(df)),        # Not available in analyzed data
            'option_type': df[option_type_col] if option_type_col else pd.Series([None] * len(df)),
            'option_exercise_style': df[exercise_style_col] if exercise_style_col else pd.Series([None] * len(df)),
            'strike_price': pd.to_numeric(df[strike_price_col], errors='coerce') if strike_price_col else pd.Series([None] * len(df)),
            'strike_price_percentage': pd.Series([None] * len(df)),      # Not available in analyzed data
            'strike_price_basis_points': pd.Series([None] * len(df)),    # Not available in analyzed data
            'strike_price_currency': pd.Series([None] * len(df)),        # Not available in analyzed data
            'delivery_type': df[delivery_col] if delivery_col else pd.Series([None] * len(df)),
            'fx_type': pd.Series([None] * len(df)),                      # Not available in analyzed data
            'other_notional_currency': pd.Series([None] * len(df)),      # Not available in analyzed data
            'competent_authority': df[competent_auth_col] if competent_auth_col else pd.Series([None] * len(df)),
            'publication_date': pd.to_datetime(df[publication_date_col], errors='coerce') if publication_date_col else pd.Series([None] * len(df))
        })
        
        option_df = option_df.dropna(subset=['isin'])
        
        if len(option_df) > 0:
            self.con.execute("""
                INSERT INTO option_instruments 
                SELECT * FROM option_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    expiry_date = EXCLUDED.expiry_date,
                    price_multiplier = EXCLUDED.price_multiplier,
                    underlying_isin = EXCLUDED.underlying_isin,
                    underlying_index_isin = EXCLUDED.underlying_index_isin,
                    underlying_index_name = EXCLUDED.underlying_index_name,
                    option_type = EXCLUDED.option_type,
                    option_exercise_style = EXCLUDED.option_exercise_style,
                    strike_price = EXCLUDED.strike_price,
                    strike_price_percentage = EXCLUDED.strike_price_percentage,
                    strike_price_basis_points = EXCLUDED.strike_price_basis_points,
                    strike_price_currency = EXCLUDED.strike_price_currency,
                    delivery_type = EXCLUDED.delivery_type,
                    fx_type = EXCLUDED.fx_type,
                    other_notional_currency = EXCLUDED.other_notional_currency,
                    competent_authority = EXCLUDED.competent_authority,
                    publication_date = EXCLUDED.publication_date
            """)
    
    def insert_forwards(self, df: pd.DataFrame):
        """Bulk insert forward instruments (J) using actual FIRDS data structure."""
        # Use actual column names from population analysis
        isin_col = self._find_column(df, ['Id'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm'])
        expiry_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_XpryDt'])
        multiplier_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_PricMltplr'])
        delivery_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_DlvryTp'])
        underlying_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm'])
        competent_auth_col = self._find_column(df, ['RefData_TechAttrbts_RlvntCmptntAuthrty'])
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt'])
        
        forward_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else pd.Series([None] * len(df)),
            'short_name': df[short_name_col] if short_name_col else pd.Series([None] * len(df)),
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else pd.Series([None] * len(df)),
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else pd.Series([None] * len(df)),
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else pd.Series([None] * len(df)),
            'underlying_index_isin': pd.Series([None] * len(df)),        # Not available in analyzed data
            'underlying_index_name': pd.Series([None] * len(df)),        # Not available in analyzed data
            'underlying_basket_isin': pd.Series([None] * len(df)),       # Not available in analyzed data
            'delivery_type': df[delivery_col] if delivery_col else pd.Series([None] * len(df)),
            'fx_type': pd.Series([None] * len(df)),                      # Not available in analyzed data
            'fx_other_notional_currency': pd.Series([None] * len(df)),   # Not available in analyzed data
            'competent_authority': df[competent_auth_col] if competent_auth_col else pd.Series([None] * len(df)),
            'publication_date': pd.to_datetime(df[publication_date_col], errors='coerce') if publication_date_col else pd.Series([None] * len(df))
        })
        
        forward_df = forward_df.dropna(subset=['isin'])
        
        if len(forward_df) > 0:
            self.con.execute("""
                INSERT INTO forward_instruments 
                SELECT * FROM forward_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    expiry_date = EXCLUDED.expiry_date,
                    price_multiplier = EXCLUDED.price_multiplier,
                    underlying_isin = EXCLUDED.underlying_isin,
                    underlying_index_isin = EXCLUDED.underlying_index_isin,
                    underlying_index_name = EXCLUDED.underlying_index_name,
                    underlying_basket_isin = EXCLUDED.underlying_basket_isin,
                    delivery_type = EXCLUDED.delivery_type,
                    fx_type = EXCLUDED.fx_type,
                    fx_other_notional_currency = EXCLUDED.fx_other_notional_currency,
                    competent_authority = EXCLUDED.competent_authority,
                    publication_date = EXCLUDED.publication_date
            """)
    
    def insert_rights(self, df: pd.DataFrame):
        """Bulk insert rights/entitlements (R) using actual FIRDS data structure."""
        # Use actual column names from population analysis
        isin_col = self._find_column(df, ['Id'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm'])
        expiry_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_XpryDt'])
        multiplier_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_PricMltplr'])
        delivery_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_DlvryTp'])
        underlying_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm'])
        competent_auth_col = self._find_column(df, ['RefData_TechAttrbts_RlvntCmptntAuthrty'])
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt'])
        
        rights_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else pd.Series([None] * len(df)),
            'short_name': df[short_name_col] if short_name_col else pd.Series([None] * len(df)),
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else pd.Series([None] * len(df)),
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else pd.Series([None] * len(df)),
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else pd.Series([None] * len(df)),
            'underlying_index_isin': pd.Series([None] * len(df)),        # Not available in analyzed data
            'underlying_index_name': pd.Series([None] * len(df)),        # Not available in analyzed data
            'underlying_index_term_unit': pd.Series([None] * len(df)),   # Not available in analyzed data
            'underlying_index_term_value': pd.Series([None] * len(df)),  # Not available in analyzed data
            'underlying_basket_isin': pd.Series([None] * len(df)),       # Not available in analyzed data
            'option_type': pd.Series([None] * len(df)),                  # Not available in analyzed data
            'option_exercise_style': pd.Series([None] * len(df)),        # Not available in analyzed data
            'strike_price': pd.Series([None] * len(df)),                 # Not available in analyzed data
            'delivery_type': df[delivery_col] if delivery_col else pd.Series([None] * len(df)),
            'fx_type': pd.Series([None] * len(df)),                      # Not available in analyzed data
            'fx_other_notional_currency': pd.Series([None] * len(df)),   # Not available in analyzed data
            'competent_authority': df[competent_auth_col] if competent_auth_col else pd.Series([None] * len(df)),
            'publication_date': pd.to_datetime(df[publication_date_col], errors='coerce') if publication_date_col else pd.Series([None] * len(df))
        })
        
        rights_df = rights_df.dropna(subset=['isin'])
        
        if len(rights_df) > 0:
            self.con.execute("""
                INSERT INTO rights_instruments 
                SELECT * FROM rights_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    expiry_date = EXCLUDED.expiry_date,
                    price_multiplier = EXCLUDED.price_multiplier,
                    underlying_isin = EXCLUDED.underlying_isin,
                    underlying_index_isin = EXCLUDED.underlying_index_isin,
                    underlying_index_name = EXCLUDED.underlying_index_name,
                    underlying_index_term_unit = EXCLUDED.underlying_index_term_unit,
                    underlying_index_term_value = EXCLUDED.underlying_index_term_value,
                    underlying_basket_isin = EXCLUDED.underlying_basket_isin,
                    option_type = EXCLUDED.option_type,
                    option_exercise_style = EXCLUDED.option_exercise_style,
                    strike_price = EXCLUDED.strike_price,
                    delivery_type = EXCLUDED.delivery_type,
                    fx_type = EXCLUDED.fx_type,
                    fx_other_notional_currency = EXCLUDED.fx_other_notional_currency,
                    competent_authority = EXCLUDED.competent_authority,
                    publication_date = EXCLUDED.publication_date
            """)
    
    def insert_civs(self, df: pd.DataFrame):
        """Bulk insert collective investment vehicles (C) using actual FIRDS data structure."""
        # Use actual column names from population analysis
        isin_col = self._find_column(df, ['Id'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm'])
        underlying_isin_col = self._find_column(df, ['RefData_DerivInstrmAttrbts_UndrlygInstrm'])
        competent_auth_col = self._find_column(df, ['RefData_TechAttrbts_RlvntCmptntAuthrty'])
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt'])
        
        civ_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else pd.Series([None] * len(df)),
            'short_name': df[short_name_col] if short_name_col else pd.Series([None] * len(df)),
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else pd.Series([None] * len(df)),
            'competent_authority': df[competent_auth_col] if competent_auth_col else pd.Series([None] * len(df)),
            'publication_date': pd.to_datetime(df[publication_date_col], errors='coerce') if publication_date_col else pd.Series([None] * len(df))
        })
        
        civ_df = civ_df.dropna(subset=['isin'])
        
        if len(civ_df) > 0:
            self.con.execute("""
                INSERT INTO civ_instruments 
                SELECT * FROM civ_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    underlying_isin = EXCLUDED.underlying_isin,
                    competent_authority = EXCLUDED.competent_authority,
                    publication_date = EXCLUDED.publication_date
            """)
    
    def insert_spots(self, df: pd.DataFrame):
        """Bulk insert spot instruments (I) using actual FIRDS data structure."""
        # Use actual column names from population analysis
        isin_col = self._find_column(df, ['Id'])
        short_name_col = self._find_column(df, ['RefData_FinInstrmGnlAttrbts_ShrtNm'])
        competent_auth_col = self._find_column(df, ['RefData_TechAttrbts_RlvntCmptntAuthrty'])
        publication_date_col = self._find_column(df, ['RefData_TechAttrbts_PblctnPrd_FrDt'])
        
        spot_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else pd.Series([None] * len(df)),
            'short_name': df[short_name_col] if short_name_col else pd.Series([None] * len(df)),
            'commodity_base_product': pd.Series([None] * len(df)),        # Not available in analyzed data
            'commodity_sub_product': pd.Series([None] * len(df)),         # Not available in analyzed data
            'commodity_additional_sub_product': pd.Series([None] * len(df)), # Not available in analyzed data
            'transaction_type': pd.Series([None] * len(df)),              # Not available in analyzed data
            'final_price_type': pd.Series([None] * len(df)),              # Not available in analyzed data
            'competent_authority': df[competent_auth_col] if competent_auth_col else pd.Series([None] * len(df)),
            'publication_date': pd.to_datetime(df[publication_date_col], errors='coerce') if publication_date_col else pd.Series([None] * len(df))
        })
        
        spot_df = spot_df.dropna(subset=['isin'])
        
        if len(spot_df) > 0:
            self.con.execute("""
                INSERT INTO spot_instruments 
                SELECT * FROM spot_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    commodity_base_product = EXCLUDED.commodity_base_product,
                    commodity_sub_product = EXCLUDED.commodity_sub_product,
                    commodity_additional_sub_product = EXCLUDED.commodity_additional_sub_product,
                    transaction_type = EXCLUDED.transaction_type,
                    final_price_type = EXCLUDED.final_price_type,
                    competent_authority = EXCLUDED.competent_authority,
                    publication_date = EXCLUDED.publication_date
            """)

