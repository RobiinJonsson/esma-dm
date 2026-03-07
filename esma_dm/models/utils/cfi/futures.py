"""CFI Category F — Futures.

Attribute decoders and labels for all futures groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import STANDARDIZATION


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class FuturesGroup(Enum):
    FINANCIAL_FUTURES = "F"
    COMMODITIES_FUTURES = "C"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

FINANCIAL_UNDERLYING: Dict[str, str] = {
    "B": "Baskets",
    "S": "Shares",
    "D": "Debt instruments",
    "C": "Currencies",
    "I": "Indices",
    "O": "Options",
    "F": "Futures",
    "W": "Swaps",
    "N": "Interest rates",
    "V": "Dividends",
    "M": "Others",
}

COMMODITY_UNDERLYING: Dict[str, str] = {
    "E": "Extraction resources",
    "A": "Agriculture",
    "I": "Industrial products",
    "S": "Services",
    "N": "Environmental",
    "P": "Polypropylene products",
    "H": "Generated resources",
    "M": "Others",
}

FUTURES_DELIVERY: Dict[str, str] = {
    "P": "Physical",
    "C": "Cash",
    "N": "Non-deliverable",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given futures group code.

    Args:
        group: Single-character group code ('F' or 'C').
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "F":
        result["underlying_assets"] = FINANCIAL_UNDERLYING.get(a1, a1)
        result["delivery"] = FUTURES_DELIVERY.get(a2, a2)
        result["standardization"] = STANDARDIZATION.get(a3, a3)

    elif group == "C":
        result["underlying_assets"] = COMMODITY_UNDERLYING.get(a1, a1)
        result["delivery"] = FUTURES_DELIVERY.get(a2, a2)
        result["standardization"] = STANDARDIZATION.get(a3, a3)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given futures group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group in ("F", "C"):
        return {
            "underlying_assets": "Underlying Assets",
            "delivery": "Delivery",
            "standardization": "Standardization",
        }
    return {}
