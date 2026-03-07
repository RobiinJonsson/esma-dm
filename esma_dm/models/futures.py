"""
Future instrument model (CFI F*).
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class FutureInstrument(DerivativeInstrument):
    """
    Future instrument (CFI F*).

    Covers exchange-traded futures on equities, interest rates, currencies,
    indices, commodities, and other underlyings.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - expiry_date / delivery_type (PHYS or CASH) / price_multiplier
      - underlying_isin / underlying_index_name / underlying_basket_isin
      - base_product / sub_product / further_sub_product (commodity futures)
      - transaction_type / final_price_type (commodity futures)
      - future_attrs.futures_value_date / future_attrs.exchange_to_traded_for
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for future instruments."""
        return super().get_schema()
