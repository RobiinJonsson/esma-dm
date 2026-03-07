"""
Non-standard derivative instrument model (CFI H*).

Covers warrants, certificates with option-like payoffs, and non-standard
(OTC) options that do not fit into listed categories.
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class NonStandardDerivativeInstrument(DerivativeInstrument):
    """
    Non-standard derivative instrument (CFI H*).

    Encompasses covered warrants, turbo warrants, inline warrants, OTC options,
    swaptions, and other derivatives that are not standardised exchange-traded
    futures or listed options.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - option_attrs.option_type (CALL / PUT)
      - option_attrs.option_style (AMER / EURO / BERM / ASIA)
      - option_attrs.strike_price_amount / strike_price_percentage / strike_price_basis_points
      - expiry_date / price_multiplier / delivery_type
      - underlying_isin / underlying_index_name / underlying_basket_isin
      - interest_rate_reference_name (swaptions)
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for non-standard derivative instruments."""
        return super().get_schema()
