"""
Forward instrument model (CFI J*).
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class ForwardInstrument(DerivativeInstrument):
    """
    Forward instrument (CFI J*).

    Covers OTC forward contracts including FX forwards, commodity forwards,
    interest rate forwards (FRAs), and equity forwards.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - fx_type / other_notional_currency (FX forwards)
      - underlying_isin / underlying_index_name / underlying_basket_isin
      - expiry_date / delivery_type (PHYS or CASH)
      - interest_rate_reference_name / term_unit / term_value (FRAs)
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for forward instruments."""
        return super().get_schema()
