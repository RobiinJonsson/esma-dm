"""
Collective investment vehicle instrument model (CFI C*).
"""
from dataclasses import dataclass
from .base import Instrument


@dataclass
class CollectiveInvestmentInstrument(Instrument):
    """
    Collective investment vehicle (CFI C*).

    Covers open-ended and closed-ended funds, ETFs (exchange-traded funds),
    REITs, UCITS funds, alternative investment funds, and money market funds.

    ETFs that are traded on exchange share many characteristics with equity
    instruments (venue data, currency, issuer) but are categorised as C* in CFI.

    All relevant FIRDS fields are in the base Instrument class:
      - isin / full_name / short_name / classification_type
      - notional_currency / issuer (fund manager LEI)
      - trading_venue (exchange or OTC admission attributes)
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for collective investment instruments."""
        return super().get_schema()
