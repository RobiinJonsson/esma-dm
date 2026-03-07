"""
Listed option instrument model (CFI O*).
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class ListedOptionInstrument(DerivativeInstrument):
    """
    Listed (exchange-traded) option instrument (CFI O*).

    Covers standardised options traded on regulated markets, including equity
    options, index options, interest rate options, currency options, and
    commodity options.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - option_attrs.option_type (CALL / PUT)
      - option_attrs.option_style (AMER / EURO / BERM / ASIA)
      - option_attrs.strike_price_amount / strike_price_percentage / strike_price_basis_points
      - expiry_date / price_multiplier / delivery_type
      - underlying_isin / underlying_index_name / underlying_basket_isin
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for listed option instruments."""
        return super().get_schema()
