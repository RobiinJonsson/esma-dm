"""
Mapper to convert raw ESMA FIRDS data to normalized instrument models.
"""
import pandas as pd
from datetime import datetime, date
from typing import Union, Optional, Dict, Any
import logging

from .base import Instrument, TradingVenueAttributes, TechnicalAttributes, RecordType
from .debt import DebtInstrument
from .equity import EquityInstrument
from .derivative import DerivativeInstrument, OptionAttributes, FutureAttributes


logger = logging.getLogger(__name__)


class InstrumentMapper:
    """
    Maps raw ESMA FIRDS data columns to normalized instrument models.
    
    Handles both FULINS and DLTINS formats and creates appropriate
    instrument subclass based on CFI code.
    """
    
    # Field mapping: raw column name -> model attribute name
    # Note: These patterns work with or without RefData_ prefix
    COMMON_FIELD_MAP = {
        # Identifiers
        'Id': 'isin',
        'FinInstrmGnlAttrbts_Id': 'isin',
        
        # Names
        'FinInstrmGnlAttrbts_FullNm': 'full_name',
        'FinInstrmGnlAttrbts_ShrtNm': 'short_name',
        'FinInstrmGnlAttrbts_ClssfctnTp': 'classification_type',
        
        # Commodity indicator
        'FinInstrmGnlAttrbts_CmmdtyDerivInd': 'commodity_derivative_indicator',
        
        # Currency and issuer
        'FinInstrmGnlAttrbts_NtnlCcy': 'notional_currency',
        'Issr': 'issuer',
        
        # Trading venue attributes
        'TradgVnRltdAttrbts_Id': 'trading_venue.venue_id',
        'TradgVnRltdAttrbts_IssrReq': 'trading_venue.issuer_request',
        'TradgVnRltdAttrbts_AdmssnApprvlDtByIssr': 'trading_venue.admission_approval_date',
        'TradgVnRltdAttrbts_ReqForAdmssnDt': 'trading_venue.request_for_admission_date',
        'TradgVnRltdAttrbts_FrstTradDt': 'trading_venue.first_trade_date',
    }
    
    DEBT_FIELD_MAP = {
        # Core debt attributes — actual FIRDS column names (after RefData_ prefix strip)
        'DebtInstrmAttrbts_TtlIssdNmnlAmt': 'total_issued_nominal_amount',
        'DebtInstrmAttrbts_MtrtyDt': 'maturity_date',
        'DebtInstrmAttrbts_NmnlValPerUnit': 'nominal_value_per_unit',
        'DebtInstrmAttrbts_IntrstRate_Fxd': 'fixed_rate',
        'DebtInstrmAttrbts_DebtSnrty': 'debt_seniority',
        # Floating rate fields — actual FIRDS column names
        'DebtInstrmAttrbts_IntrstRate_Fltg_RefRate_ISIN': 'floating_rate_reference_isin',
        'DebtInstrmAttrbts_IntrstRate_Fltg_RefRate_Indx': 'floating_rate_reference_index',
        'DebtInstrmAttrbts_IntrstRate_Fltg_RefRate_Nm': 'floating_rate_reference_name',
        'DebtInstrmAttrbts_IntrstRate_Fltg_Term_Unit': 'floating_rate_term_unit',
        'DebtInstrmAttrbts_IntrstRate_Fltg_Term_Val': 'floating_rate_term_value',
        'DebtInstrmAttrbts_IntrstRate_Fltg_BsisPtSprd': 'floating_rate_basis_points',
    }

    @staticmethod
    def _normalize_column_name(col: str) -> str:
        """Normalize column name by removing common prefixes."""
        # Remove RefData_ prefix if present
        if col.startswith('RefData_'):
            col = col[8:]  # len('RefData_') = 8
        return col

    EQUITY_FIELD_MAP = {
        # Structured equity only: underlying instrument (e.g. CFI EY*)
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN': 'underlying_instrument',
    }

    DERIVATIVE_FIELD_MAP = {
        # Common derivative fields — actual FIRDS column names
        'DerivInstrmAttrbts_XpryDt': 'expiry_date',
        'DerivInstrmAttrbts_PricMltplr': 'price_multiplier',
        'DerivInstrmAttrbts_DlvryTp': 'delivery_type',

        # Underlying instrument references
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN': 'underlying_isin',
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_LEI': 'underlying_lei',
        'DerivInstrmAttrbts_UndrlygInstrm_Bskt_ISIN': 'underlying_basket_isin',
        'DerivInstrmAttrbts_UndrlygInstrm_Bskt_LEI': 'underlying_basket_lei',

        # Underlying index references
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_ISIN': 'underlying_index_isin',
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Nm': 'underlying_index_name',
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Indx': 'underlying_index_reference_index',
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_Term_Unit': 'underlying_index_term_unit',
        'DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_Term_Val': 'underlying_index_term_value',

        # Option attributes
        'DerivInstrmAttrbts_OptnTp': 'option_attrs.option_type',
        'DerivInstrmAttrbts_OptnExrcStyle': 'option_attrs.option_style',
        'DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Amt': 'option_attrs.strike_price_amount',
        'DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Sgn': 'option_attrs.strike_price_sign',
        'DerivInstrmAttrbts_StrkPric_Pric_Pctg': 'option_attrs.strike_price_percentage',
        'DerivInstrmAttrbts_StrkPric_Pric_BsisPts': 'option_attrs.strike_price_basis_points',
        'DerivInstrmAttrbts_StrkPric_NoPric_Pdg': 'option_attrs.no_price_condition',
        'DerivInstrmAttrbts_StrkPric_NoPric_Ccy': 'option_attrs.no_price_currency',

        # FX-specific attributes
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_FxTp': 'fx_type',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_OthrNtnlCcy': 'other_notional_currency',

        # Interest rate swap attributes
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Nm': 'interest_rate_reference_name',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Indx': 'interest_rate_reference_index',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_IntrstRate_Term_Unit': 'interest_rate_term_unit',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_IntrstRate_Term_Val': 'interest_rate_term_value',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_FrstLegIntrstRate_Fxd': 'first_leg_rate_fixed',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Intrst_OthrLegIntrstRate_Fxd': 'other_leg_rate_fixed',

        # Commodity attributes — transaction/price type (asset-class agnostic)
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_TxTp': 'transaction_type',
        'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_FnlPricTp': 'final_price_type',
    }
    
    @staticmethod
    def _parse_date(value: Any) -> Optional[date]:
        """Parse date from string or return None."""
        if pd.isna(value) or value is None or value == '':
            return None
        
        if isinstance(value, date):
            return value
        
        if isinstance(value, datetime):
            return value.date()
        
        # Try parsing string dates in different formats
        str_value = str(value).strip()
        
        # ISO 8601 with time (2023-06-01T06:00:00Z)
        if 'T' in str_value:
            try:
                return datetime.fromisoformat(str_value.replace('Z', '+00:00')).date()
            except ValueError:
                pass
        
        # Standard date formats
        for fmt in ('%Y-%m-%d', '%Y%m%d', '%d/%m/%Y', '%m/%d/%Y'):
            try:
                return datetime.strptime(str_value, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"Failed to parse date: {value}")
        return None
    
    @staticmethod
    def _parse_float(value: Any) -> Optional[float]:
        """Parse float from string or return None."""
        if pd.isna(value) or value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse float: {value}")
            return None
    
    @staticmethod
    def _parse_int(value: Any) -> Optional[int]:
        """Parse int from string or return None."""
        if pd.isna(value) or value is None or value == '':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse int: {value}")
            return None
    
    @staticmethod
    def _parse_string(value: Any) -> Optional[str]:
        """Parse string and return None for empty/null values."""
        if pd.isna(value) or value is None or value == '':
            return None
        return str(value).strip()
    
    @staticmethod
    def _parse_bool(value: Any) -> Optional[bool]:
        """Parse boolean from string or return None."""
        if pd.isna(value) or value is None or value == '':
            return None
        str_val = str(value).strip().lower()
        if str_val in ('true', '1', 'yes', 't', 'y'):
            return True
        elif str_val in ('false', '0', 'no', 'f', 'n'):
            return False
        return None
    
    @staticmethod
    def _parse_record_type(row: pd.Series) -> Optional[RecordType]:
        """Determine record type from DLTINS fields."""
        if 'ModfdRcrd_New' in row and row['ModfdRcrd_New'] == 'true':
            return RecordType.MODIFIED
        if 'NewRcrd' in row and row['NewRcrd'] == 'true':
            return RecordType.NEW
        if 'TermntdRcrd' in row and row['TermntdRcrd'] == 'true':
            return RecordType.TERMINATED
        return None
    
    @classmethod
    def _find_column(cls, row: pd.Series, *possible_names: str) -> Optional[str]:
        """Find a column by trying multiple possible names (with/without prefixes)."""
        # Cache index for faster lookups
        if not hasattr(row, '_column_cache'):
            row._column_cache = set(row.index)
        
        for name in possible_names:
            if name in row._column_cache:
                return name
            # Try with RefData_ prefix
            prefixed = f"RefData_{name}"
            if prefixed in row._column_cache:
                return prefixed
        return None
    
    @classmethod
    def _get_value(cls, row: pd.Series, *possible_names: str) -> Any:
        """Get value from row by trying multiple column name variants."""
        col = cls._find_column(row, *possible_names)
        return row[col] if col else None
    
    @classmethod
    def _extract_common_fields(cls, row: pd.Series) -> Dict[str, Any]:
        """Extract common fields from row."""
        fields = {}
        
        # Map common fields - try both with and without RefData_ prefix
        for raw_field, model_field in cls.COMMON_FIELD_MAP.items():
            # Skip nested attributes for now
            if '.' in model_field:
                continue
            
            value = cls._get_value(row, raw_field)
            if value is not None:
                fields[model_field] = cls._parse_string(value)
        
        # Handle record type specially
        fields['record_type'] = cls._parse_record_type(row)
        
        return fields
    
    @classmethod
    def _extract_trading_venue_attrs(cls, row: pd.Series) -> TradingVenueAttributes:
        """Extract trading venue attributes from row."""
        return TradingVenueAttributes(
            venue_id=cls._parse_string(cls._get_value(row, 'TradgVnRltdAttrbts_Id')),
            issuer_request=cls._parse_bool(cls._get_value(row, 'TradgVnRltdAttrbts_IssrReq')),
            admission_approval_date=cls._parse_date(cls._get_value(row, 'TradgVnRltdAttrbts_AdmssnApprvlDtByIssr')),
            request_for_admission_date=cls._parse_date(cls._get_value(row, 'TradgVnRltdAttrbts_ReqForAdmssnDt')),
            first_trade_date=cls._parse_date(cls._get_value(row, 'TradgVnRltdAttrbts_FrstTradDt')),
            termination_date=cls._parse_date(cls._get_value(row, 'TradgVnRltdAttrbts_TermntnDt')),
        )
    
    @classmethod
    def _extract_technical_attrs(cls, row: pd.Series) -> TechnicalAttributes:
        """Extract technical attributes from row."""
        return TechnicalAttributes(
            relevant_competent_authority=cls._parse_string(
                cls._get_value(row, 'TechRcrdAttrbts_RlvntCmptntAuthrty', 'TechAttrbts_RlvntCmptntAuthrty')
            ),
            publication_period_from=cls._parse_date(
                cls._get_value(row, 'TechRcrdAttrbts_PblctnPrd_FrDt', 'TechAttrbts_PblctnPrd_FrDt')
            ),
            relevant_trading_venue=cls._parse_string(
                cls._get_value(row, 'TechRcrdAttrbts_RlvntTradgVn', 'TechAttrbts_RlvntTradgVn')
            ),
            never_published=cls._parse_bool(cls._get_value(row, 'TechRcrdAttrbts_NvrPblshd')),
        )
    
    @classmethod
    def _extract_debt_fields(cls, row: pd.Series) -> Dict[str, Any]:
        """Extract debt-specific fields from row."""
        fields = {}
        
        for raw_field, model_field in cls.DEBT_FIELD_MAP.items():
            value = cls._get_value(row, raw_field)
            if value is None:
                continue
            
            # Handle type conversion based on field
            if 'date' in model_field.lower() or 'dt' in raw_field.lower():
                fields[model_field] = cls._parse_date(value)
            elif 'amt' in raw_field.lower() or 'rate' in model_field.lower() or 'val' in raw_field.lower():
                fields[model_field] = cls._parse_float(value)
            elif 'term_value' in model_field:
                fields[model_field] = cls._parse_int(value)
            else:
                fields[model_field] = cls._parse_string(value)
        
        # Determine interest rate type
        if fields.get('fixed_rate') is not None:
            fields['interest_rate_type'] = 'fixed'
        elif fields.get('floating_rate_reference_isin') or fields.get('floating_rate_reference_index'):
            fields['interest_rate_type'] = 'floating'
        
        return fields
    
    @classmethod
    def _extract_equity_fields(cls, row: pd.Series) -> Dict[str, Any]:
        """Extract equity-specific fields from row."""
        fields = {}
        
        for raw_field, model_field in cls.EQUITY_FIELD_MAP.items():
            value = cls._get_value(row, raw_field)
            if value is not None:
                fields[model_field] = cls._parse_string(value)
        
        return fields
    
    @classmethod
    def _extract_option_attrs(cls, row: pd.Series) -> Optional[OptionAttributes]:
        """Extract option attributes from row."""
        option_type = cls._parse_string(cls._get_value(row, 'DerivInstrmAttrbts_OptnTp'))
        option_style = cls._parse_string(cls._get_value(row, 'DerivInstrmAttrbts_OptnExrcStyle'))
        strike_amount = cls._parse_float(
            cls._get_value(row, 'DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Amt')
        )
        strike_pct = cls._parse_float(
            cls._get_value(row, 'DerivInstrmAttrbts_StrkPric_Pric_Pctg')
        )
        strike_bps = cls._parse_float(
            cls._get_value(row, 'DerivInstrmAttrbts_StrkPric_Pric_BsisPts')
        )

        if option_type or option_style or strike_amount is not None or strike_pct is not None:
            return OptionAttributes(
                option_type=option_type,
                option_style=option_style,
                strike_price_amount=strike_amount,
                strike_price_sign=cls._parse_string(
                    cls._get_value(row, 'DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Sgn')
                ),
                strike_price_percentage=strike_pct,
                strike_price_basis_points=strike_bps,
                no_price_condition=cls._parse_string(
                    cls._get_value(row, 'DerivInstrmAttrbts_StrkPric_NoPric_Pdg')
                ),
                no_price_currency=cls._parse_string(
                    cls._get_value(row, 'DerivInstrmAttrbts_StrkPric_NoPric_Ccy')
                ),
            )

        return None

    @classmethod
    def _extract_future_attrs(cls, row: pd.Series) -> Optional[FutureAttributes]:
        """Extract future attributes from row."""
        delivery_type = cls._parse_string(cls._get_value(row, 'DerivInstrmAttrbts_DlvryTp'))
        futures_value_date = cls._parse_date(cls._get_value(row, 'DerivInstrmAttrbts_ValDtOfTheFutr'))

        if delivery_type or futures_value_date:
            return FutureAttributes(
                delivery_type=delivery_type,
                futures_value_date=futures_value_date,
                exchange_to_traded_for=cls._parse_string(cls._get_value(row, 'DerivInstrmAttrbts_XchgToTradgFr')),
            )

        return None

    @classmethod
    def _extract_commodity_product(cls, row: pd.Series) -> tuple:
        """Scan row for commodity base/sub/further-sub product fields.

        ESMA encodes commodity products under asset-class-specific column names
        (e.g. AsstClssSpcfcAttrbts_Cmmdty_Pdct_Nrgy_Oil_BasePdct). This method
        scans the row index for any column matching the pattern and returns the
        first non-null triple found.

        Returns:
            Tuple of (base_product, sub_product, further_sub_product).
        """
        base = sub = further = None
        prefix = 'DerivInstrmAttrbts_AsstClssSpcfcAttrbts_Cmmdty_Pdct_'
        for col in row.index:
            normalized = cls._normalize_column_name(col)
            if not normalized.startswith(prefix):
                continue
            rest = normalized[len(prefix):]
            parts = rest.split('_')
            if parts[-1] == 'BasePdct' and base is None:
                base = cls._parse_string(row[col])
            elif parts[-1] == 'SubPdct' and sub is None:
                sub = cls._parse_string(row[col])
            elif parts[-1] == 'AddtlSubPdct' and further is None:
                further = cls._parse_string(row[col])
        return base, sub, further
    
    @classmethod
    def _extract_derivative_fields(cls, row: pd.Series) -> Dict[str, Any]:
        """Extract derivative-specific fields from row."""
        fields = {}

        for raw_field, model_field in cls.DERIVATIVE_FIELD_MAP.items():
            # Option/future nested attrs are handled separately
            if model_field.startswith('option_attrs.') or model_field.startswith('future_attrs.'):
                continue

            value = cls._get_value(row, raw_field)
            if value is None:
                continue

            if 'date' in model_field.lower() or raw_field.lower().endswith('dt'):
                fields[model_field] = cls._parse_date(value)
            elif model_field in ('price_multiplier',) or 'term_val' in raw_field.lower():
                fields[model_field] = cls._parse_float(value)
            else:
                fields[model_field] = cls._parse_string(value)

        # Commodity product fields: scan row for all asset-class-specific product columns
        base, sub, further = cls._extract_commodity_product(row)
        if base is not None:
            fields['base_product'] = base
        if sub is not None:
            fields['sub_product'] = sub
        if further is not None:
            fields['further_sub_product'] = further

        # Extract nested attributes
        fields['option_attrs'] = cls._extract_option_attrs(row)
        fields['future_attrs'] = cls._extract_future_attrs(row)

        return fields
    
    @classmethod
    def from_row(cls, row: pd.Series) -> Optional[Instrument]:
        """
        Create an Instrument instance from a DataFrame row.
        
        Args:
            row: DataFrame row with raw ESMA FIRDS data
            
        Returns:
            Appropriate Instrument subclass based on CFI code, or None if required fields missing
        """
        try:
            # Extract common fields
            common = cls._extract_common_fields(row)
            
            # Check required fields
            if not common.get('isin'):
                logger.debug("Skipping row: missing ISIN")
                return None
            
            if not common.get('full_name'):
                # Use short name or ISIN as fallback
                common['full_name'] = common.get('short_name') or common.get('isin') or 'Unknown'
            
            # Extract nested attributes
            common['trading_venue'] = cls._extract_trading_venue_attrs(row)
            common['technical'] = cls._extract_technical_attrs(row)
            
            # Determine instrument type from CFI code
            cfi_code = common.get('classification_type', '')
            if not cfi_code or len(cfi_code) == 0:
                # Default to base Instrument if no CFI code
                return Instrument(**common)
            
            asset_type = cfi_code[0]
            
            # Create appropriate subclass
            if asset_type == 'D':
                # Debt instrument
                debt_fields = cls._extract_debt_fields(row)
                return DebtInstrument(**{**common, **debt_fields})
            
            elif asset_type == 'E':
                # Equity instrument
                equity_fields = cls._extract_equity_fields(row)
                return EquityInstrument(**{**common, **equity_fields})
            
            elif asset_type in ('F', 'I', 'J', 'S', 'H'):
                # Derivative instrument
                deriv_fields = cls._extract_derivative_fields(row)
                return DerivativeInstrument(**{**common, **deriv_fields})
            
            else:
                # Other instrument types (C, O, R, etc.) - use base Instrument
                return Instrument(**common)
        
        except Exception as e:
            logger.debug(f"Error creating instrument: {e}")
            return None
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> list[Instrument]:
        """
        Create list of Instrument instances from a DataFrame.
        
        Args:
            df: DataFrame with raw ESMA FIRDS data
            
        Returns:
            List of Instrument instances
        """
        instruments = []
        
        for idx, row in df.iterrows():
            try:
                instrument = cls.from_row(row)
                instruments.append(instrument)
            except Exception as e:
                logger.error(f"Failed to map row {idx}: {e}")
                continue
        
        return instruments
