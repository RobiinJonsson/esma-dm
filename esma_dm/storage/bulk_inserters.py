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
    
    def insert_equities(self, df: pd.DataFrame):
        """Bulk insert equity instruments (E)."""
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
    
    def insert_debt(self, df: pd.DataFrame):
        """Bulk insert debt instruments (D)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        maturity_col = self._find_column(df, ['MtrtyDt', 'maturity_date'])
        total_issued_col = self._find_column(df, ['TtlIssdNmnlAmt', 'total_issued_nominal'])
        nominal_col = self._find_column(df, ['MnmNmnlQt', 'NmnlValPerUnit', 'nominal_value'])
        fixed_rate_col = self._find_column(df, ['IntrstRate_Fxd', 'fixed_rate'])
        float_ref_col = self._find_column(df, ['IntrstRate_Fltg_RefRate_Nm', 'floating_rate_reference'])
        float_idx_col = self._find_column(df, ['IntrstRate_Fltg_RefRate_Indx', 'floating_rate_index'])
        float_term_unit_col = self._find_column(df, ['IntrstRate_Fltg_Term_Unit'])
        float_term_val_col = self._find_column(df, ['IntrstRate_Fltg_Term_Val'])
        float_spread_col = self._find_column(df, ['IntrstRate_Fltg_BsisPtSprd'])
        seniority_col = self._find_column(df, ['DebtSnrty', 'debt_seniority'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        debt_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'maturity_date': pd.to_datetime(df[maturity_col], errors='coerce') if maturity_col else None,
            'total_issued_nominal_amount': pd.to_numeric(df[total_issued_col], errors='coerce') if total_issued_col else None,
            'nominal_value_per_unit': pd.to_numeric(df[nominal_col], errors='coerce') if nominal_col else None,
            'interest_rate_type': 'FIXED' if fixed_rate_col and not pd.isna(df[fixed_rate_col]).all() else 'FLOATING' if float_ref_col else None,
            'fixed_rate': pd.to_numeric(df[fixed_rate_col], errors='coerce') if fixed_rate_col else None,
            'floating_rate_reference': df[float_ref_col] if float_ref_col else None,
            'floating_rate_index': df[float_idx_col] if float_idx_col else None,
            'floating_rate_term_unit': df[float_term_unit_col] if float_term_unit_col else None,
            'floating_rate_term_value': df[float_term_val_col] if float_term_val_col else None,
            'floating_rate_basis_spread': pd.to_numeric(df[float_spread_col], errors='coerce') if float_spread_col else None,
            'debt_seniority': df[seniority_col] if seniority_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
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
                    floating_rate_index = EXCLUDED.floating_rate_index,
                    floating_rate_term_unit = EXCLUDED.floating_rate_term_unit,
                    floating_rate_term_value = EXCLUDED.floating_rate_term_value,
                    floating_rate_basis_spread = EXCLUDED.floating_rate_basis_spread,
                    debt_seniority = EXCLUDED.debt_seniority,
                    delivery_type = EXCLUDED.delivery_type,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_futures(self, df: pd.DataFrame):
        """Bulk insert futures instruments (F)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        expiry_col = self._find_column(df, ['XpryDt', 'expiry_date'])
        multiplier_col = self._find_column(df, ['PricMltplr', 'price_multiplier'])
        underlying_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_ISIN'])
        underlying_index_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        base_product_col = self._find_column(df, ['Cmmdty_Pdct', 'BasePdct'])
        sub_product_col = self._find_column(df, ['SubPdct'])
        addtl_sub_product_col = self._find_column(df, ['AddtlSubPdct'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        futures_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else None,
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'underlying_index_name': df[underlying_index_col] if underlying_index_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
            'commodity_base_product': df[base_product_col] if base_product_col else None,
            'commodity_sub_product': df[sub_product_col] if sub_product_col else None,
            'commodity_additional_sub_product': df[addtl_sub_product_col] if addtl_sub_product_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
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
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_options(self, df: pd.DataFrame):
        """Bulk insert option instruments (O, H)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        expiry_col = self._find_column(df, ['XpryDt', 'expiry_date'])
        multiplier_col = self._find_column(df, ['PricMltplr', 'price_multiplier'])
        underlying_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_ISIN'])
        underlying_index_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_ISIN'])
        underlying_index_name_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm'])
        option_type_col = self._find_column(df, ['OptnTp', 'option_type'])
        exercise_style_col = self._find_column(df, ['OptnExrcStyle', 'exercise_style'])
        strike_price_col = self._find_column(df, ['StrkPric_Pric_MntryVal_Amt'])
        strike_pct_col = self._find_column(df, ['StrkPric_Pric_Pctg'])
        strike_bp_col = self._find_column(df, ['StrkPric_Pric_BsisPts'])
        strike_ccy_col = self._find_column(df, ['StrkPric_NoPric_Ccy'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        fx_type_col = self._find_column(df, ['FX_FxTp'])
        other_ccy_col = self._find_column(df, ['FX_OthrNtnlCcy'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        option_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else None,
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'underlying_index_isin': df[underlying_index_isin_col] if underlying_index_isin_col else None,
            'underlying_index_name': df[underlying_index_name_col] if underlying_index_name_col else None,
            'option_type': df[option_type_col] if option_type_col else None,
            'option_exercise_style': df[exercise_style_col] if exercise_style_col else None,
            'strike_price': pd.to_numeric(df[strike_price_col], errors='coerce') if strike_price_col else None,
            'strike_price_percentage': pd.to_numeric(df[strike_pct_col], errors='coerce') if strike_pct_col else None,
            'strike_price_basis_points': pd.to_numeric(df[strike_bp_col], errors='coerce') if strike_bp_col else None,
            'strike_price_currency': df[strike_ccy_col] if strike_ccy_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
            'fx_type': df[fx_type_col] if fx_type_col else None,
            'other_notional_currency': df[other_ccy_col] if other_ccy_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
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
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_swaps(self, df: pd.DataFrame):
        """Bulk insert swap instruments (S)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        expiry_col = self._find_column(df, ['XpryDt', 'expiry_date'])
        multiplier_col = self._find_column(df, ['PricMltplr', 'price_multiplier'])
        underlying_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_ISIN'])
        underlying_lei_col = self._find_column(df, ['UndrlygInstrm_Sngl_LEI'])
        underlying_index_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_ISIN'])
        underlying_index_name_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm_RefRate_Nm', 'UndrlygInstrm_Sngl_Indx_Nm'])
        underlying_index_term_unit_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm_Term_Unit', 'UndrlygInstrm_Sngl_Indx_Trm_Trm_Unit'])
        underlying_index_term_value_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm_Term_Val', 'UndrlygInstrm_Sngl_Indx_Trm_Trm_Val'])
        underlying_basket_col = self._find_column(df, ['UndrlygInstrm_Bskt_ISIN'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        ir_ref_col = self._find_column(df, ['AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Nm', 'IntrstRate_Ref_Nm'])
        ir_term_unit_col = self._find_column(df, ['AsstClssSpcfcAttrbts_Intrst_IntrstRate_Term_Unit', 'IntrstRate_Trm_Unit'])
        ir_term_value_col = self._find_column(df, ['AsstClssSpcfcAttrbts_Intrst_IntrstRate_Term_Val', 'IntrstRate_Trm_Val'])
        fx_other_ccy_col = self._find_column(df, ['AsstClssSpcfcAttrbts_FX_OthrNtnlCcy', 'FX_OthrNtnlCcy'])
        venue_col = self._find_column(df, ['TradgVnRltdAttrbts_Id', 'TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['TradgVnRltdAttrbts_FrstTradDt', 'FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TradgVnRltdAttrbts_TermntnDt', 'TermntnDt', 'termination_date'])
        
        swap_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else None,
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'underlying_lei': df[underlying_lei_col] if underlying_lei_col else None,
            'underlying_index_isin': df[underlying_index_isin_col] if underlying_index_isin_col else None,
            'underlying_index_name': df[underlying_index_name_col] if underlying_index_name_col else None,
            'underlying_index_term_unit': df[underlying_index_term_unit_col] if underlying_index_term_unit_col else None,
            'underlying_index_term_value': df[underlying_index_term_value_col] if underlying_index_term_value_col else None,
            'underlying_basket_isin': df[underlying_basket_col] if underlying_basket_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
            'interest_rate_reference_name': df[ir_ref_col] if ir_ref_col else None,
            'interest_rate_term_unit': df[ir_term_unit_col] if ir_term_unit_col else None,
            'interest_rate_term_value': df[ir_term_value_col] if ir_term_value_col else None,
            'fx_other_notional_currency': df[fx_other_ccy_col] if fx_other_ccy_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
        })
        
        swap_df = swap_df.dropna(subset=['isin'])
        
        if len(swap_df) > 0:
            self.con.execute("""
                INSERT INTO swap_instruments 
                SELECT * FROM swap_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    expiry_date = EXCLUDED.expiry_date,
                    price_multiplier = EXCLUDED.price_multiplier,
                    underlying_isin = EXCLUDED.underlying_isin,
                    underlying_lei = EXCLUDED.underlying_lei,
                    underlying_index_isin = EXCLUDED.underlying_index_isin,
                    underlying_index_name = EXCLUDED.underlying_index_name,
                    underlying_index_term_unit = EXCLUDED.underlying_index_term_unit,
                    underlying_index_term_value = EXCLUDED.underlying_index_term_value,
                    underlying_basket_isin = EXCLUDED.underlying_basket_isin,
                    delivery_type = EXCLUDED.delivery_type,
                    interest_rate_reference_name = EXCLUDED.interest_rate_reference_name,
                    interest_rate_term_unit = EXCLUDED.interest_rate_term_unit,
                    interest_rate_term_value = EXCLUDED.interest_rate_term_value,
                    fx_other_notional_currency = EXCLUDED.fx_other_notional_currency,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_forwards(self, df: pd.DataFrame):
        """Bulk insert forward instruments (J)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        expiry_col = self._find_column(df, ['XpryDt', 'expiry_date'])
        multiplier_col = self._find_column(df, ['PricMltplr', 'price_multiplier'])
        underlying_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_ISIN'])
        underlying_index_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_ISIN'])
        underlying_index_name_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm'])
        underlying_basket_col = self._find_column(df, ['UndrlygInstrm_Bskt_ISIN'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        fx_type_col = self._find_column(df, ['FX_FxTp'])
        fx_other_ccy_col = self._find_column(df, ['FX_OthrNtnlCcy'])
        commodity_base_col = self._find_column(df, ['Cmmdty_BasePdct'])
        commodity_sub_col = self._find_column(df, ['Cmmdty_SubPdct'])
        commodity_add_col = self._find_column(df, ['Cmmdty_AddtlSubPdct'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        forward_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else None,
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'underlying_index_isin': df[underlying_index_isin_col] if underlying_index_isin_col else None,
            'underlying_index_name': df[underlying_index_name_col] if underlying_index_name_col else None,
            'underlying_basket_isin': df[underlying_basket_col] if underlying_basket_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
            'fx_type': df[fx_type_col] if fx_type_col else None,
            'fx_other_notional_currency': df[fx_other_ccy_col] if fx_other_ccy_col else None,
            'commodity_base_product': df[commodity_base_col] if commodity_base_col else None,
            'commodity_sub_product': df[commodity_sub_col] if commodity_sub_col else None,
            'commodity_additional_sub_product': df[commodity_add_col] if commodity_add_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
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
                    commodity_base_product = EXCLUDED.commodity_base_product,
                    commodity_sub_product = EXCLUDED.commodity_sub_product,
                    commodity_additional_sub_product = EXCLUDED.commodity_additional_sub_product,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_rights(self, df: pd.DataFrame):
        """Bulk insert rights/entitlements (R)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        expiry_col = self._find_column(df, ['XpryDt', 'expiry_date'])
        multiplier_col = self._find_column(df, ['PricMltplr', 'price_multiplier'])
        underlying_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_ISIN'])
        underlying_index_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_ISIN'])
        underlying_index_name_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Nm'])
        underlying_index_term_unit_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Trm_Trm_Unit'])
        underlying_index_term_value_col = self._find_column(df, ['UndrlygInstrm_Sngl_Indx_Trm_Trm_Val'])
        underlying_basket_col = self._find_column(df, ['UndrlygInstrm_Bskt_ISIN'])
        option_type_col = self._find_column(df, ['OptnTp', 'option_type'])
        exercise_style_col = self._find_column(df, ['OptnExrcStyle', 'exercise_style'])
        strike_price_col = self._find_column(df, ['StrkPric_Pric_MntryVal_Amt'])
        delivery_col = self._find_column(df, ['DlvryTp', 'delivery_type'])
        commodity_base_col = self._find_column(df, ['Cmmdty_BasePdct'])
        commodity_sub_col = self._find_column(df, ['Cmmdty_SubPdct'])
        commodity_add_col = self._find_column(df, ['Cmmdty_AddtlSubPdct'])
        fx_type_col = self._find_column(df, ['FX_FxTp'])
        fx_other_ccy_col = self._find_column(df, ['FX_OthrNtnlCcy'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        rights_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'expiry_date': pd.to_datetime(df[expiry_col], errors='coerce') if expiry_col else None,
            'price_multiplier': pd.to_numeric(df[multiplier_col], errors='coerce') if multiplier_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'underlying_index_isin': df[underlying_index_isin_col] if underlying_index_isin_col else None,
            'underlying_index_name': df[underlying_index_name_col] if underlying_index_name_col else None,
            'underlying_index_term_unit': df[underlying_index_term_unit_col] if underlying_index_term_unit_col else None,
            'underlying_index_term_value': df[underlying_index_term_value_col] if underlying_index_term_value_col else None,
            'underlying_basket_isin': df[underlying_basket_col] if underlying_basket_col else None,
            'option_type': df[option_type_col] if option_type_col else None,
            'option_exercise_style': df[exercise_style_col] if exercise_style_col else None,
            'strike_price': pd.to_numeric(df[strike_price_col], errors='coerce') if strike_price_col else None,
            'delivery_type': df[delivery_col] if delivery_col else None,
            'commodity_base_product': df[commodity_base_col] if commodity_base_col else None,
            'commodity_sub_product': df[commodity_sub_col] if commodity_sub_col else None,
            'commodity_additional_sub_product': df[commodity_add_col] if commodity_add_col else None,
            'fx_type': df[fx_type_col] if fx_type_col else None,
            'fx_other_notional_currency': df[fx_other_ccy_col] if fx_other_ccy_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
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
                    commodity_base_product = EXCLUDED.commodity_base_product,
                    commodity_sub_product = EXCLUDED.commodity_sub_product,
                    commodity_additional_sub_product = EXCLUDED.commodity_additional_sub_product,
                    fx_type = EXCLUDED.fx_type,
                    fx_other_notional_currency = EXCLUDED.fx_other_notional_currency,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_civs(self, df: pd.DataFrame):
        """Bulk insert collective investment vehicles (C)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        underlying_isin_col = self._find_column(df, ['UndrlygInstrm_Sngl_ISIN'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        civ_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'underlying_isin': df[underlying_isin_col] if underlying_isin_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
        })
        
        civ_df = civ_df.dropna(subset=['isin'])
        
        if len(civ_df) > 0:
            self.con.execute("""
                INSERT INTO civ_instruments 
                SELECT * FROM civ_df
                ON CONFLICT (isin) DO UPDATE SET
                    short_name = EXCLUDED.short_name,
                    underlying_isin = EXCLUDED.underlying_isin,
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)
    
    def insert_spots(self, df: pd.DataFrame):
        """Bulk insert spot instruments (I)."""
        isin_col = self._find_column(df, ['Id', 'ISIN'])
        short_name_col = self._find_column(df, ['ShrtNm', 'short_name'])
        commodity_base_col = self._find_column(df, ['Cmmdty_BasePdct'])
        commodity_sub_col = self._find_column(df, ['Cmmdty_SubPdct'])
        commodity_add_col = self._find_column(df, ['Cmmdty_AddtlSubPdct'])
        transaction_type_col = self._find_column(df, ['Cmmdty_TxTp'])
        price_type_col = self._find_column(df, ['Cmmdty_FnlPricTp'])
        venue_col = self._find_column(df, ['TradgVnId', 'trading_venue_id'])
        first_trade_col = self._find_column(df, ['FrstTradDt', 'first_trade_date'])
        term_col = self._find_column(df, ['TermntnDt', 'termination_date'])
        
        spot_df = pd.DataFrame({
            'isin': df[isin_col] if isin_col else None,
            'short_name': df[short_name_col] if short_name_col else None,
            'commodity_base_product': df[commodity_base_col] if commodity_base_col else None,
            'commodity_sub_product': df[commodity_sub_col] if commodity_sub_col else None,
            'commodity_additional_sub_product': df[commodity_add_col] if commodity_add_col else None,
            'transaction_type': df[transaction_type_col] if transaction_type_col else None,
            'final_price_type': df[price_type_col] if price_type_col else None,
            'trading_venue_id': df[venue_col] if venue_col else None,
            'first_trade_date': pd.to_datetime(df[first_trade_col], errors='coerce') if first_trade_col else None,
            'termination_date': pd.to_datetime(df[term_col], errors='coerce') if term_col else None
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
                    trading_venue_id = EXCLUDED.trading_venue_id,
                    first_trade_date = EXCLUDED.first_trade_date,
                    termination_date = EXCLUDED.termination_date
            """)

