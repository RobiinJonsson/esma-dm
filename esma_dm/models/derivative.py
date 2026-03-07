"""
Derivative instrument models for options, futures, and other derivatives.
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional
from .base import Instrument


@dataclass
class OptionAttributes:
    """Attributes specific to option contracts."""

    option_type: Optional[str] = None
    """Option type: CALL or PUT (DerivInstrmAttrbts_OptnTp)"""

    option_style: Optional[str] = None
    """Exercise style: AMER=American, EURO=European, BERM=Bermudan, ASIA=Asian"""

    strike_price_amount: Optional[float] = None
    """Strike price as monetary amount (DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Amt)"""

    strike_price_sign: Optional[str] = None
    """Sign of monetary strike price: PLUS or MINU (DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Sgn)"""

    strike_price_percentage: Optional[float] = None
    """Strike price as a percentage (DerivInstrmAttrbts_StrkPric_Pric_Pctg)"""

    strike_price_basis_points: Optional[float] = None
    """Strike price in basis points (DerivInstrmAttrbts_StrkPric_Pric_BsisPts)"""

    no_price_condition: Optional[str] = None
    """Condition when no price set, e.g. PNDG=Pending (DerivInstrmAttrbts_StrkPric_NoPric_Pdg)"""

    no_price_currency: Optional[str] = None
    """Currency associated with a no-price condition (DerivInstrmAttrbts_StrkPric_NoPric_Ccy)"""

    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for option attributes."""
        return {
            'option_type': {'type': 'str', 'description': 'Option type: CALL or PUT'},
            'option_style': {'type': 'str', 'description': 'Exercise style: AMER, EURO, BERM, ASIA'},
            'strike_price_amount': {'type': 'float', 'description': 'Strike price as monetary amount'},
            'strike_price_sign': {'type': 'str', 'description': 'Sign of monetary strike: PLUS or MINU'},
            'strike_price_percentage': {'type': 'float', 'description': 'Strike price as percentage'},
            'strike_price_basis_points': {'type': 'float', 'description': 'Strike price in basis points'},
            'no_price_condition': {'type': 'str', 'description': 'No-price condition code, e.g. PNDG'},
            'no_price_currency': {'type': 'str', 'description': 'Currency for no-price condition'},
        }


@dataclass
class FutureAttributes:
    """Attributes specific to future contracts."""

    delivery_type: Optional[str] = None
    """Delivery type: PHYS=Physical, CASH=Cash (DerivInstrmAttrbts_DlvryTp)"""

    futures_value_date: Optional[date] = None
    """Value date for the future contract"""

    exchange_to_traded_for: Optional[str] = None
    """Exchange to traded for (XOFF code)"""

    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for future attributes."""
        return {
            'delivery_type': {'type': 'str', 'description': 'Delivery type: PHYS=Physical, CASH=Cash'},
            'futures_value_date': {'type': 'date', 'description': 'Value date for the future contract'},
            'exchange_to_traded_for': {'type': 'str', 'description': 'Exchange to traded for (XOFF code)'},
        }


@dataclass
class DerivativeInstrument(Instrument):
    """
    Derivative instrument (futures, options, swaps, forwards, etc.).

    Covers CFI codes starting with: F (Futures), H (Options/Warrants),
    I (Spot/ETCs), J (Forwards), S (Swaps).
    """

    # Common derivative fields
    expiry_date: Optional[date] = None
    """Expiration date (DerivInstrmAttrbts_XpryDt)"""

    price_multiplier: Optional[float] = None
    """Price multiplier (DerivInstrmAttrbts_PricMltplr)"""

    delivery_type: Optional[str] = None
    """Delivery type: PHYS=Physical, CASH=Cash (DerivInstrmAttrbts_DlvryTp)"""

    # Underlying — single instrument
    underlying_isin: Optional[str] = None
    """ISIN of the underlying single instrument (DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN)"""

    underlying_lei: Optional[str] = None
    """LEI of the underlying single entity (DerivInstrmAttrbts_UndrlygInstrm_Sngl_LEI)"""

    # Underlying — basket
    underlying_basket_isin: Optional[str] = None
    """ISIN of the underlying basket (DerivInstrmAttrbts_UndrlygInstrm_Bskt_ISIN)"""

    underlying_basket_lei: Optional[str] = None
    """LEI of the underlying basket entity (DerivInstrmAttrbts_UndrlygInstrm_Bskt_LEI)"""

    # Underlying — index / reference rate
    underlying_index_isin: Optional[str] = None
    """ISIN of the underlying index (DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_ISIN)"""

    underlying_index_name: Optional[str] = None
    """Name of the underlying index or reference rate (DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Nm)"""

    underlying_index_reference_index: Optional[str] = None
    """Reference index code e.g. EIBO (DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Indx)"""

    underlying_index_term_unit: Optional[str] = None
    """Term unit of the underlying index, e.g. MNTH (DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_Term_Unit)"""

    underlying_index_term_value: Optional[str] = None
    """Term value of the underlying index (DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_Term_Val)"""

    # FX-specific attributes
    fx_type: Optional[str] = None
    """FX transaction type (AsstClssSpcfcAttrbts_FX_FxTp)"""

    other_notional_currency: Optional[str] = None
    """Second notional currency for FX/multi-currency derivatives (AsstClssSpcfcAttrbts_FX_OthrNtnlCcy)"""

    # Interest rate swap attributes
    interest_rate_reference_name: Optional[str] = None
    """Interest rate reference name (AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Nm)"""

    interest_rate_reference_index: Optional[str] = None
    """Interest rate reference index code (AsstClssSpcfcAttrbts_Intrst_IntrstRate_RefRate_Indx)"""

    interest_rate_term_unit: Optional[str] = None
    """Term unit for interest rate (AsstClssSpcfcAttrbts_Intrst_IntrstRate_Term_Unit)"""

    interest_rate_term_value: Optional[str] = None
    """Term value for interest rate (AsstClssSpcfcAttrbts_Intrst_IntrstRate_Term_Val)"""

    first_leg_rate_fixed: Optional[float] = None
    """Fixed rate on the first leg of the swap (AsstClssSpcfcAttrbts_Intrst_FrstLegIntrstRate_Fxd)"""

    other_leg_rate_fixed: Optional[float] = None
    """Fixed rate on the other leg of the swap (AsstClssSpcfcAttrbts_Intrst_OthrLegIntrstRate_Fxd)"""

    # Commodity derivative attributes
    base_product: Optional[str] = None
    """Base product for commodity derivatives (AsstClssSpcfcAttrbts_Cmmdty_Pdct_*_BasePdct)"""

    sub_product: Optional[str] = None
    """Sub-product for commodity derivatives (AsstClssSpcfcAttrbts_Cmmdty_Pdct_*_SubPdct)"""

    further_sub_product: Optional[str] = None
    """Further sub-product for commodity derivatives (AsstClssSpcfcAttrbts_Cmmdty_Pdct_*_AddtlSubPdct)"""

    transaction_type: Optional[str] = None
    """Transaction type for commodity derivatives (AsstClssSpcfcAttrbts_Cmmdty_TxTp)"""

    final_price_type: Optional[str] = None
    """Final price type for commodity derivatives (AsstClssSpcfcAttrbts_Cmmdty_FnlPricTp)"""

    # Nested attributes for specific derivative types
    option_attrs: Optional[OptionAttributes] = None
    """Option-specific attributes (populated for CFI H* and O* instruments)"""

    future_attrs: Optional[FutureAttributes] = None
    """Future-specific attributes (populated for CFI F* instruments)"""

    @property
    def is_option(self) -> bool:
        """Check if this is an option derivative."""
        cfi = self.classification_type or ''
        return cfi.startswith('H') or cfi.startswith('O')

    @property
    def is_future(self) -> bool:
        """Check if this is a future derivative."""
        cfi = self.classification_type or ''
        return cfi.startswith('F')

    @property
    def is_swap(self) -> bool:
        """Check if this is a swap derivative."""
        cfi = self.classification_type or ''
        return cfi.startswith('S')

    @property
    def is_forward(self) -> bool:
        """Check if this is a forward contract."""
        cfi = self.classification_type or ''
        return cfi.startswith('J')

    @property
    def is_commodity_derivative(self) -> bool:
        """Check if this is a commodity derivative."""
        return self.base_product is not None

    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for derivative-specific fields."""
        base_schema = super().get_schema()
        derivative_schema = {
            'expiry_date': {'type': 'date', 'description': 'Expiration date of the derivative'},
            'price_multiplier': {'type': 'float', 'description': 'Price multiplier'},
            'delivery_type': {'type': 'str', 'description': 'Delivery type: PHYS or CASH'},
            'underlying_isin': {'type': 'str', 'description': 'ISIN of the single underlying instrument'},
            'underlying_lei': {'type': 'str', 'description': 'LEI of the single underlying entity'},
            'underlying_basket_isin': {'type': 'str', 'description': 'ISIN of the underlying basket'},
            'underlying_basket_lei': {'type': 'str', 'description': 'LEI of the underlying basket entity'},
            'underlying_index_isin': {'type': 'str', 'description': 'ISIN of the underlying index'},
            'underlying_index_name': {'type': 'str', 'description': 'Name of the underlying index or reference rate'},
            'underlying_index_reference_index': {'type': 'str', 'description': 'Reference index code (e.g. EIBO)'},
            'underlying_index_term_unit': {'type': 'str', 'description': 'Term unit of the underlying index'},
            'underlying_index_term_value': {'type': 'str', 'description': 'Term value of the underlying index'},
            'fx_type': {'type': 'str', 'description': 'FX transaction type'},
            'other_notional_currency': {'type': 'str', 'description': 'Second notional currency'},
            'interest_rate_reference_name': {'type': 'str', 'description': 'Interest rate reference rate name'},
            'interest_rate_reference_index': {'type': 'str', 'description': 'Interest rate reference index code'},
            'interest_rate_term_unit': {'type': 'str', 'description': 'Interest rate term unit'},
            'interest_rate_term_value': {'type': 'str', 'description': 'Interest rate term value'},
            'first_leg_rate_fixed': {'type': 'float', 'description': 'Fixed rate on first swap leg'},
            'other_leg_rate_fixed': {'type': 'float', 'description': 'Fixed rate on other swap leg'},
            'base_product': {'type': 'str', 'description': 'Commodity base product'},
            'sub_product': {'type': 'str', 'description': 'Commodity sub-product'},
            'further_sub_product': {'type': 'str', 'description': 'Commodity further sub-product'},
            'transaction_type': {'type': 'str', 'description': 'Commodity transaction type'},
            'final_price_type': {'type': 'str', 'description': 'Commodity final price type'},
        }
        return {**base_schema, **derivative_schema}
