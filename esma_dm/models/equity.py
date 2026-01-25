"""
Equity instrument model for shares and equity securities.
"""
from dataclasses import dataclass
from typing import Optional
from .base import Instrument


@dataclass
class EquityInstrument(Instrument):
    """
    Equity instrument (shares, stocks, etc.) based on actual FIRDS data structure.
    
    Covers CFI codes starting with 'E' (Equities).
    Most equity data is in the base Instrument class. Very few equity-specific fields exist in FIRDS.
    """
    
    # Actual equity-specific attributes from FIRDS
    underlying_instrument: Optional[str] = None
    """Underlying instrument ISIN (RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN) - for equity derivatives"""
    
    commodity_derivative_indicator: Optional[bool] = None
    """Commodity derivative indicator (RefData_FinInstrmGnlAttrbts_CmmdtyDerivInd)"""
    """Capital investment restrictions"""
    
    @property
    def is_commodity_derivative(self) -> bool:
        """Check if this equity is a commodity derivative."""
        return self.commodity_derivative_indicator is True
    
    @property
    def has_underlying(self) -> bool:
        """Check if this equity has an underlying instrument."""
        return self.underlying_instrument is not None
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for equity-specific fields based on actual FIRDS data."""
        base_schema = super().get_schema()
        equity_schema = {
            'underlying_instrument': {'type': 'str', 'description': 'Underlying instrument ISIN (for equity derivatives)'},
            'commodity_derivative_indicator': {'type': 'bool', 'description': 'Commodity derivative indicator from FIRDS'},
        }
        return {**base_schema, **equity_schema}
