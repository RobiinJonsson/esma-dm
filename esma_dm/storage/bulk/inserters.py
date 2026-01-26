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
        
        # Map processed DataFrame columns to instruments table schema
        instruments_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'cfi_code': df['cfi_code'] if 'cfi_code' in df.columns else pd.Series([None] * len(df)),
            'instrument_type': df['asset_type'] if 'asset_type' in df.columns else pd.Series([None] * len(df)),
            'issuer': df['issuer'] if 'issuer' in df.columns else pd.Series([None] * len(df)),
            'full_name': df['full_name'] if 'full_name' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'currency': df['currency'] if 'currency' in df.columns else pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df)),
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
            try:
                # Use INSERT OR IGNORE to handle duplicates
                insert_query = """
                    INSERT OR IGNORE INTO instruments 
                    (isin, cfi_code, instrument_type, issuer, full_name, short_name, currency,
                     competent_authority, publication_date,
                     valid_from_date, valid_to_date, latest_record_flag, record_type,
                     version_number, source_file, source_file_type, last_update_timestamp,
                     inconsistency_indicator, indexed_at)
                    SELECT * FROM instruments_temp
                """
                
                # Create temporary view and insert
                self.con.execute("CREATE OR REPLACE VIEW instruments_temp AS SELECT * FROM instruments_df")
                self.con.execute(insert_query)
                return len(instruments_df)
            except Exception as e:
                self.logger.error(f"Failed to insert instruments: {e}")
                return 0
        else:
            self.logger.warning("No valid main instruments to insert")
            return 0
    
    def insert_equities(self, df: pd.DataFrame):
        """Bulk insert equity instruments (E) using processed DataFrame structure."""
        # Add debugging
        
        # Only insert equity-specific fields (general fields go in main instruments table)
        # Based on actual FIRDS data structure
        equity_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_instrument': df['underlying_instrument'] if 'underlying_instrument' in df.columns else pd.Series([None] * len(df)),
            'commodity_derivative_indicator': df['commodity_derivative_indicator'] if 'commodity_derivative_indicator' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))  # Default version number for current mode
        })
        
        equity_df = equity_df.dropna(subset=['isin'])
        
        if len(equity_df) > 0:
            try:
                self.con.register('equity_df', equity_df)
                self.con.execute("""
                    INSERT INTO equity_instruments 
                    SELECT * FROM equity_df
                    ON CONFLICT (isin) DO UPDATE SET
                        underlying_instrument = EXCLUDED.underlying_instrument,
                        commodity_derivative_indicator = EXCLUDED.commodity_derivative_indicator,
                        version_number = EXCLUDED.version_number
                """)
            except Exception as e:
                self.logger.error(f"Insert failed: {e}")
        else:
            self.logger.debug("No data to insert after filtering")
    
    def insert_debt(self, df: pd.DataFrame):
        """Bulk insert debt instruments (D) using actual FIRDS data structure."""
        
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
        
        if len(debt_df) > 0:
            try:
                self.con.register('debt_df', debt_df)
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
            except Exception as e:
                self.logger.error(f"Insert failed: {e}")
        else:
            self.logger.debug("No data to insert after filtering")

    def insert_swaps(self, df: pd.DataFrame):
        """Bulk insert swap instruments (S) using actual FIRDS data structure."""
        
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
        
        if len(swap_df) > 0:
            try:
                self.con.register('swap_df', swap_df)
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
            except Exception as e:
                self.logger.error(f"Insert failed: {e}")
        else:
            self.logger.debug("No data to insert after filtering")
    
    def insert_futures(self, df: pd.DataFrame):
        """Bulk insert futures instruments (F). Expects DataFrame with data model column names."""
        futures_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'expiry_date': df['expiry_date'] if 'expiry_date' in df.columns else pd.Series([None] * len(df)),
            'price_multiplier': df['price_multiplier'] if 'price_multiplier' in df.columns else pd.Series([None] * len(df)),
            'underlying_isin': df['underlying_isin'] if 'underlying_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_name': df['underlying_index_name'] if 'underlying_index_name' in df.columns else pd.Series([None] * len(df)),
            'delivery_type': df['delivery_type'] if 'delivery_type' in df.columns else pd.Series([None] * len(df)),
            'commodity_base_product': df['commodity_base_product'] if 'commodity_base_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_sub_product': df['commodity_sub_product'] if 'commodity_sub_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_additional_sub_product': df['commodity_additional_sub_product'] if 'commodity_additional_sub_product' in df.columns else pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df))
        })
        
        futures_df = futures_df.dropna(subset=['isin'])
        
        if len(futures_df) > 0:
            self.con.register('futures_df', futures_df)
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
        """Bulk insert option instruments (O). Expects DataFrame with data model column names."""
        # Select relevant columns from master_df for option_instruments table
        option_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'expiry_date': df['expiry_date'] if 'expiry_date' in df.columns else pd.Series([None] * len(df)),
            'price_multiplier': df['price_multiplier'] if 'price_multiplier' in df.columns else pd.Series([None] * len(df)),
            'underlying_isin': df['underlying_isin'] if 'underlying_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_isin': df['underlying_index_isin'] if 'underlying_index_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_name': df['underlying_index_name'] if 'underlying_index_name' in df.columns else pd.Series([None] * len(df)),
            'option_type': df['option_type'] if 'option_type' in df.columns else pd.Series([None] * len(df)),
            'option_exercise_style': df['option_exercise_style'] if 'option_exercise_style' in df.columns else pd.Series([None] * len(df)),
            'strike_price': df['strike_price'] if 'strike_price' in df.columns else pd.Series([None] * len(df)),
            'strike_price_percentage': df['strike_price_percentage'] if 'strike_price_percentage' in df.columns else pd.Series([None] * len(df)),
            'strike_price_basis_points': df['strike_price_basis_points'] if 'strike_price_basis_points' in df.columns else pd.Series([None] * len(df)),
            'strike_price_currency': pd.Series([None] * len(df)),
            'delivery_type': df['delivery_type'] if 'delivery_type' in df.columns else pd.Series([None] * len(df)),
            'fx_type': pd.Series([None] * len(df)),
            'other_notional_currency': pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        option_df = option_df.dropna(subset=['isin'])
        
        if len(option_df) > 0:
            try:
                self.con.register('option_df', option_df)
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
            except Exception as e:
                raise RuntimeError(f"Failed to insert option instruments: {e}")
    
    def insert_forwards(self, df: pd.DataFrame):
        """Bulk insert forward instruments (J). Expects DataFrame with data model column names."""
        forward_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'expiry_date': df['expiry_date'] if 'expiry_date' in df.columns else pd.Series([None] * len(df)),
            'price_multiplier': df['price_multiplier'] if 'price_multiplier' in df.columns else pd.Series([None] * len(df)),
            'underlying_isin': df['underlying_isin'] if 'underlying_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_isin': df['underlying_index_isin'] if 'underlying_index_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_name': df['underlying_index_name'] if 'underlying_index_name' in df.columns else pd.Series([None] * len(df)),
            'underlying_basket_isin': df['underlying_basket_isin'] if 'underlying_basket_isin' in df.columns else pd.Series([None] * len(df)),
            'delivery_type': df['delivery_type'] if 'delivery_type' in df.columns else pd.Series([None] * len(df)),
            'fx_type': df['fx_type'] if 'fx_type' in df.columns else pd.Series([None] * len(df)),
            'fx_other_notional_currency': df['fx_other_notional_currency'] if 'fx_other_notional_currency' in df.columns else pd.Series([None] * len(df)),
            'commodity_base_product': df['commodity_base_product'] if 'commodity_base_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_sub_product': df['commodity_sub_product'] if 'commodity_sub_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_additional_sub_product': df['commodity_additional_sub_product'] if 'commodity_additional_sub_product' in df.columns else pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        forward_df = forward_df.dropna(subset=['isin'])
        
        if len(forward_df) > 0:
            self.con.register('forward_df', forward_df)
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
        """Bulk insert rights/entitlements (H). Expects DataFrame with data model column names."""
        rights_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'expiry_date': df['expiry_date'] if 'expiry_date' in df.columns else pd.Series([None] * len(df)),
            'price_multiplier': df['price_multiplier'] if 'price_multiplier' in df.columns else pd.Series([None] * len(df)),
            'underlying_isin': df['underlying_isin'] if 'underlying_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_isin': df['underlying_index_isin'] if 'underlying_index_isin' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_name': df['underlying_index_name'] if 'underlying_index_name' in df.columns else pd.Series([None] * len(df)),
            'underlying_index_term_unit': pd.Series([None] * len(df)),
            'underlying_index_term_value': pd.Series([None] * len(df)),
            'underlying_basket_isin': df['underlying_basket_isin'] if 'underlying_basket_isin' in df.columns else pd.Series([None] * len(df)),
            'option_type': df['option_type'] if 'option_type' in df.columns else pd.Series([None] * len(df)),
            'option_exercise_style': df['option_exercise_style'] if 'option_exercise_style' in df.columns else pd.Series([None] * len(df)),
            'strike_price': df['strike_price'] if 'strike_price' in df.columns else pd.Series([None] * len(df)),
            'delivery_type': df['delivery_type'] if 'delivery_type' in df.columns else pd.Series([None] * len(df)),
            'commodity_base_product': df['commodity_base_product'] if 'commodity_base_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_sub_product': df['commodity_sub_product'] if 'commodity_sub_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_additional_sub_product': df['commodity_additional_sub_product'] if 'commodity_additional_sub_product' in df.columns else pd.Series([None] * len(df)),
            'fx_type': df['fx_type'] if 'fx_type' in df.columns else pd.Series([None] * len(df)),
            'fx_other_notional_currency': df['fx_other_notional_currency'] if 'fx_other_notional_currency' in df.columns else pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        rights_df = rights_df.dropna(subset=['isin'])
        
        if len(rights_df) > 0:
            self.con.register('rights_df', rights_df)
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
        """Bulk insert collective investment vehicles (C). Expects DataFrame with data model column names."""
        civ_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'underlying_isin': df['underlying_isin'] if 'underlying_isin' in df.columns else pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        civ_df = civ_df.dropna(subset=['isin'])
        
        if len(civ_df) > 0:
            self.con.register('civ_df', civ_df)
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
        """Bulk insert spot instruments (I/R). Expects DataFrame with data model column names."""
        spot_df = pd.DataFrame({
            'isin': df['isin'] if 'isin' in df.columns else pd.Series([None] * len(df)),
            'short_name': df['short_name'] if 'short_name' in df.columns else pd.Series([None] * len(df)),
            'commodity_base_product': df['commodity_base_product'] if 'commodity_base_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_sub_product': df['commodity_sub_product'] if 'commodity_sub_product' in df.columns else pd.Series([None] * len(df)),
            'commodity_additional_sub_product': df['commodity_additional_sub_product'] if 'commodity_additional_sub_product' in df.columns else pd.Series([None] * len(df)),
            'transaction_type': pd.Series([None] * len(df)),
            'final_price_type': pd.Series([None] * len(df)),
            'competent_authority': df['competent_authority'] if 'competent_authority' in df.columns else pd.Series([None] * len(df)),
            'publication_date': df['publication_date'] if 'publication_date' in df.columns else pd.Series([None] * len(df)),
            'version_number': pd.Series([1] * len(df))
        })
        
        spot_df = spot_df.dropna(subset=['isin'])
        
        if len(spot_df) > 0:
            self.con.register('spot_df', spot_df)
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

