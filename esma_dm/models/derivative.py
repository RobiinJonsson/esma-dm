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
    """Option type: CALL or PUT"""
    
    strike_price: Optional[float] = None
    """Strike price of the option"""
    
    strike_price_currency: Optional[str] = None
    """Currency of the strike price"""
    
    option_style: Optional[str] = None
    """Exercise style: AMER=American, EURO=European, BERM=Bermudan, ASIA=Asian"""
    
    option_exercise_date: Optional[date] = None
    """Exercise date for the option"""
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for option attributes."""
        return {
            'option_type': {'type': 'str', 'description': 'Option type: CALL or PUT'},
            'strike_price': {'type': 'float', 'description': 'Strike price of the option'},
            'strike_price_currency': {'type': 'str', 'description': 'Currency of the strike price'},
            'option_style': {'type': 'str', 'description': 'Exercise style: AMER=American, EURO=European, BERM=Bermudan, ASIA=Asian'},
            'option_exercise_date': {'type': 'date', 'description': 'Exercise date for the option'},
        }


@dataclass
class FutureAttributes:
    """Attributes specific to future contracts."""
    
    delivery_type: Optional[str] = None
    """Delivery type: PHYS=Physical, CASH=Cash"""
    
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
    Derivative instrument (futures, options, swaps, etc.) with derivative-specific attributes.
    
    Covers CFI codes starting with: F (Futures), I (Options), J (Forwards), S (Swaps), H (Others)
    """
    
    # Common derivative attributes
    expiry_date: Optional[date] = None
    """Expiration date of the derivative"""
    
    price_multiplier: Optional[float] = None
    """Price multiplier for the derivative"""
    
    underlying_isin: Optional[str] = None
    """ISIN of the underlying instrument"""
    
    underlying_index_name: Optional[str] = None
    """Name of the underlying index (if applicable)"""
    
    underlying_index_term_value: Optional[str] = None
    """Term value of the underlying index"""
    
    underlying_index_term_unit: Optional[str] = None
    """Term unit of the underlying index"""
    
    notional_currency_1: Optional[str] = None
    """First notional currency (for multi-currency derivatives)"""
    
    notional_currency_2: Optional[str] = None
    """Second notional currency (for multi-currency derivatives)"""
    
    # Commodity derivative attributes
    base_product: Optional[str] = None
    """Base product for commodity derivatives"""
    
    sub_product: Optional[str] = None
    """Sub-product for commodity derivatives"""
    
    further_sub_product: Optional[str] = None
    """Further sub-product for commodity derivatives"""
    
    transaction_type: Optional[str] = None
    """Transaction type for commodity derivatives"""
    
    final_price_type: Optional[str] = None
    """Final price type for commodity derivatives"""
    
    # Nested attributes for specific derivative types
    option_attrs: Optional[OptionAttributes] = None
    """Option-specific attributes (when asset_type is 'I')"""
    
    future_attrs: Optional[FutureAttributes] = None
    """Future-specific attributes (when asset_type is 'F')"""
    
    @property
    def is_option(self) -> bool:
        """Check if this is an option derivative."""
        return self.asset_type == 'I'
    
    @property
    def is_future(self) -> bool:
        """Check if this is a future derivative."""
        return self.asset_type == 'F'
    
    @property
    def is_swap(self) -> bool:
        """Check if this is a swap derivative."""
        return self.asset_type == 'S'
    
    @property
    def is_commodity_derivative(self) -> bool:
        """Check if this is a commodity derivative."""
        return self.commodity_derivative_indicator == 'true'
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for derivative-specific fields."""
        base_schema = super().get_schema()
        derivative_schema = {
            'expiry_date': {'type': 'date', 'description': 'Expiration date of the derivative'},
            'price_multiplier': {'type': 'float', 'description': 'Price multiplier for the derivative'},
            'underlying_isin': {'type': 'str', 'description': 'ISIN of the underlying instrument'},
            'underlying_index_name': {'type': 'str', 'description': 'Name of the underlying index (if applicable)'},
            'underlying_index_term_value': {'type': 'str', 'description': 'Term value of the underlying index'},
            'underlying_index_term_unit': {'type': 'str', 'description': 'Term unit of the underlying index'},
            'notional_currency_1': {'type': 'str', 'description': 'First notional currency (for multi-currency derivatives)'},
            'notional_currency_2': {'type': 'str', 'description': 'Second notional currency (for multi-currency derivatives)'},
            'base_product': {'type': 'str', 'description': 'Base product for commodity derivatives'},
            'sub_product': {'type': 'str', 'description': 'Sub-product for commodity derivatives'},
            'further_sub_product': {'type': 'str', 'description': 'Further sub-product for commodity derivatives'},
            'transaction_type': {'type': 'str', 'description': 'Transaction type for commodity derivatives'},
            'final_price_type': {'type': 'str', 'description': 'Final price type for commodity derivatives'},
            'option_attrs': {'type': 'OptionAttributes', 'description': 'Option-specific attributes (when asset_type is I)'},
            'future_attrs': {'type': 'FutureAttributes', 'description': 'Future-specific attributes (when asset_type is F)'},
        }
        return {**base_schema, **derivative_schema}
