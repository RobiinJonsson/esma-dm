"""
Referential instrument model (CFI T*).
"""
from dataclasses import dataclass
from .base import Instrument


@dataclass
class ReferentialInstrument(Instrument):
    """
    Referential instrument (CFI T*).

    Covers currencies (spot FX codes), interest rate benchmarks, commodity
    indices used as underlyings, and other referential instruments that exist
    as reference entities in FIRDS but are not directly tradeable securities.

    These appear in FIRDS primarily because they are referenced as underlyings
    of derivative instruments and require an ISIN for identification.

    All relevant FIRDS fields are in the base Instrument class:
      - isin / full_name / short_name / classification_type
      - notional_currency / issuer
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for referential instruments."""
        return super().get_schema()
