"""
Financing instrument model (CFI L*).
"""
from dataclasses import dataclass
from .base import Instrument


@dataclass
class FinancingInstrument(Instrument):
    """
    Financing instrument (CFI L*).

    Covers repurchase agreements (repos), reverse repos, securities lending
    transactions, buy-sell-back agreements, and other collateralised financing
    products. These are short-term money-market-equivalent instruments used
    primarily for liquidity management.

    FIRDS coverage for L* instruments is limited; the base Instrument fields
    capture the key identifiers:
      - isin / full_name / short_name / classification_type
      - notional_currency / issuer
      - trading_venue
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for financing instruments."""
        return super().get_schema()
