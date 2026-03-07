"""
Swap instrument model (CFI S*).
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class SwapInstrument(DerivativeInstrument):
    """
    Swap instrument (CFI S*).

    Covers interest rate swaps, equity swaps, currency swaps, credit default
    swaps, total return swaps, and other swap types.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - interest_rate_reference_name / index / term_unit / term_value
      - first_leg_rate_fixed / other_leg_rate_fixed
      - fx_type / other_notional_currency
      - underlying_isin / underlying_index_name / underlying_basket_isin
      - expiry_date / delivery_type
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for swap instruments."""
        return super().get_schema()
