"""CFI Category J — Forwards.

Attribute decoders and labels for all forward groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import PAYOUT_TRIGGER


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class ForwardsGroup(Enum):
    EQUITY = "E"
    FOREIGN_EXCHANGE = "F"
    CREDIT = "C"
    RATES = "R"
    COMMODITIES = "T"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

EQUITY_UNDERLYING: Dict[str, str] = {
    "S": "Single stock",
    "I": "Index",
    "B": "Basket",
    "O": "Options",
    "F": "Futures",
}

FX_UNDERLYING: Dict[str, str] = {
    "T": "Spot — single",
    "R": "Forward — single",
    "V": "Spot — basket",
    "U": "Spot — index",
    "W": "Forward — basket",
    "S": "Forward — index",
    "O": "Option — single",
    "K": "Option — basket",
    "J": "Option — index",
    "F": "Futures — single",
    "N": "Futures — basket",
    "L": "Futures — index",
}

FX_PAYOUT: Dict[str, str] = {
    "C": "Contract for difference (CFD)",
    "S": "Spread bet",
    "F": "Forward price",
    "R": "Rolling spot",
}

CREDIT_UNDERLYING: Dict[str, str] = {
    "A": "Single name",
    "I": "Index",
    "B": "Basket",
    "C": "CDS — single name",
    "D": "CDS — index",
    "G": "CDS — basket",
    "O": "Options",
}

RATES_UNDERLYING: Dict[str, str] = {
    "I": "Interest rate index",
    "O": "Options",
    "M": "Others",
}

COMMODITY_UNDERLYING: Dict[str, str] = {
    "A": "Agriculture",
    "B": "Basket — single commodity",
    "C": "Basket — multiple commodities",
    "G": "Freight",
    "H": "Index — multiple commodities",
    "I": "Index — single commodity",
    "J": "Energy",
    "K": "Metals",
    "N": "Environmental",
    "P": "Polypropylene products",
    "S": "Fertilizer",
    "T": "Paper",
    "M": "Others",
}

FORWARD_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
}

FORWARD_PAYOUT: Dict[str, str] = {
    "C": "Contract for difference (CFD)",
    "S": "Spread bet",
    "F": "Forward price",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given forward group code.

    Args:
        group: Single-character group code.
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "E":
        result["underlying"] = EQUITY_UNDERLYING.get(a1, a1)
        result["payout_trigger"] = FORWARD_PAYOUT.get(a3, a3)
        result["delivery"] = FORWARD_DELIVERY.get(a4, a4)

    elif group == "F":
        result["underlying"] = FX_UNDERLYING.get(a1, a1)
        result["payout_trigger"] = FX_PAYOUT.get(a3, a3)
        result["delivery"] = FORWARD_DELIVERY.get(a4, a4)

    elif group == "C":
        result["underlying"] = CREDIT_UNDERLYING.get(a1, a1)
        result["payout_trigger"] = FORWARD_PAYOUT.get(a3, a3)
        result["delivery"] = FORWARD_DELIVERY.get(a4, a4)

    elif group == "R":
        result["underlying"] = RATES_UNDERLYING.get(a1, a1)
        result["payout_trigger"] = FORWARD_PAYOUT.get(a3, a3)
        result["delivery"] = FORWARD_DELIVERY.get(a4, a4)

    elif group == "T":
        result["underlying"] = COMMODITY_UNDERLYING.get(a1, a1)
        result["payout_trigger"] = FORWARD_PAYOUT.get(a3, a3)
        result["delivery"] = FORWARD_DELIVERY.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given forward group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group in ("E", "F", "C", "R", "T"):
        return {
            "underlying": "Underlying",
            "payout_trigger": "Payout/Trigger",
            "delivery": "Delivery",
        }
    return {}
