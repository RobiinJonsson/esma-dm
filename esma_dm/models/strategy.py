"""
Strategy instrument model (CFI K*) — multi-leg and combination strategies.
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class StrategyInstrument(DerivativeInstrument):
    """
    Strategy instrument (CFI K*).

    Covers multi-leg derivative strategies such as straddles, strangles,
    butterflies, condors, spreads, and other combination positions that are
    represented as a single instrument in FIRDS.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - underlying_isin / underlying_index_name / underlying_basket_isin
      - expiry_date / delivery_type / price_multiplier
      - option_attrs (when the strategy contains option legs)
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for strategy instruments."""
        return super().get_schema()
