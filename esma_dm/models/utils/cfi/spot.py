"""CFI Category I — Spot instruments.

Attribute decoders and labels for all spot groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class SpotGroup(Enum):
    FOREIGN_EXCHANGE = "F"
    COMMODITIES = "T"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

SPOT_FX_DELIVERY: Dict[str, str] = {
    "P": "Physical",
}

SPOT_COMMODITY_UNDERLYING: Dict[str, str] = {
    "A": "Agriculture",
    "J": "Energy",
    "K": "Metals",
    "N": "Environmental",
    "P": "Polypropylene products",
    "S": "Fertilizer",
    "T": "Paper",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given spot group code.

    Args:
        group: Single-character group code ('F' or 'T').
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "F":
        result["delivery"] = SPOT_FX_DELIVERY.get(a4, a4)

    elif group == "T":
        result["underlying_assets"] = SPOT_COMMODITY_UNDERLYING.get(a1, a1)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given spot group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group == "F":
        return {"delivery": "Delivery"}
    if group == "T":
        return {"underlying_assets": "Underlying Assets"}
    return {}
