"""
Entitlement instrument model (CFI R*).

Covers rights, warrants (not listed options), subscription rights,
mini-futures, and other entitlement instruments.
"""
from dataclasses import dataclass
from .base import Instrument


@dataclass
class EntitlementInstrument(Instrument):
    """
    Entitlement instrument (CFI R*).

    Covers subscription rights, allotment rights, mini-futures / leverage
    certificates (RF*), knock-out products, covered warrants that grant the
    right but not the obligation to subscribe for shares, and other rights
    attached to securities.

    These instruments are listed and traded on-exchange, often with an
    underlying equity, index, commodity, or currency basket.

    All relevant FIRDS fields are in the base Instrument class:
      - isin / full_name / short_name / classification_type
      - notional_currency / issuer
      - trading_venue (exchange admission data)
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for entitlement instruments."""
        return super().get_schema()
