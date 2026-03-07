"""
Spot instrument model (CFI I*) — exchange-traded commodities and spot products.
"""
from dataclasses import dataclass
from .derivative import DerivativeInstrument


@dataclass
class SpotInstrument(DerivativeInstrument):
    """
    Spot instrument (CFI I*).

    Covers exchange-traded commodities (ETCs), spot FX instruments, and other
    products that represent a spot claim on a physical commodity or currency.
    ETCs are exchange-traded products backed by physical metal, energy, or
    agricultural commodities.

    Key FIRDS fields (all inherited from DerivativeInstrument):
      - underlying_isin / underlying_index_name / underlying_basket_isin
      - base_product / sub_product / further_sub_product (commodity type)
      - fx_type / other_notional_currency (FX spot instruments)
      - delivery_type (PHYS or CASH)
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for spot instruments."""
        return super().get_schema()
