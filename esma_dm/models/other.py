"""
Other / miscellaneous instrument model (CFI M*).
"""
from dataclasses import dataclass
from .base import Instrument


@dataclass
class OtherInstrument(Instrument):
    """
    Other / miscellaneous instrument (CFI M*).

    Catch-all category for instruments that cannot be classified under any
    of the 13 defined CFI categories. Includes hybrid instruments, complex
    structured products with no standard category, and instruments pending
    precise classification.

    All relevant FIRDS fields are in the base Instrument class:
      - isin / full_name / short_name / classification_type
      - notional_currency / issuer
      - trading_venue
    """

    @classmethod
    def get_schema(cls) -> dict:
        """Get full schema for other / miscellaneous instruments."""
        return super().get_schema()
