"""CFI Category R — Entitlements (rights).

Attribute decoders and labels for all entitlement groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import EXERCISE_STYLE_EAB, FORM


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class EntitlementsGroup(Enum):
    ALLOTMENT_RIGHTS = "A"
    SUBSCRIPTION_RIGHTS = "S"
    PURCHASE_RIGHTS = "P"
    WARRANTS = "W"
    MINI_FUTURES = "F"
    DEPOSITORY_RECEIPTS_ON_ENTITLEMENTS = "D"
    OTHERS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

ASSETS_R: Dict[str, str] = {
    "S": "Equities",
    "P": "Debt instruments",
    "C": "Currencies",
    "F": "CIVs",
    "B": "Baskets",
    "I": "Indices",
    "M": "Others",
}

WARRANT_UNDERLYING: Dict[str, str] = {
    "B": "Baskets",
    "S": "Shares",
    "D": "Debt instruments",
    "T": "Commodities",
    "C": "Currencies",
    "I": "Indices",
    "M": "Others",
}

WARRANT_TYPE: Dict[str, str] = {
    "T": "Traditional",
    "N": "Naked/uncovered",
    "C": "Covered",
}

CALL_PUT: Dict[str, str] = {
    "C": "Call",
    "P": "Put",
    "B": "Call and put",
}

MINI_BARRIER: Dict[str, str] = {
    "T": "Underlying price-based",
    "N": "Instrument price-based",
    "M": "Others",
}

MINI_DIRECTION: Dict[str, str] = {
    "C": "Long (call/bull)",
    "P": "Short (put/bear)",
    "M": "Others",
}

EXERCISE_STYLE_EABM: Dict[str, str] = {
    "E": "European",
    "A": "American",
    "B": "Bermudan",
    "M": "Others",
}

DEPOSITORY_DEPENDENCY: Dict[str, str] = {
    "A": "Allotment rights",
    "S": "Subscription rights",
    "P": "Purchase rights",
    "W": "Warrants",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given entitlements group code.

    Args:
        group: Single-character group code.
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "A":
        result["form"] = FORM.get(a4, a4)

    elif group in ("S", "P"):
        result["assets"] = ASSETS_R.get(a1, a1)
        result["form"] = FORM.get(a4, a4)

    elif group == "W":
        result["underlying_assets"] = WARRANT_UNDERLYING.get(a1, a1)
        result["type"] = WARRANT_TYPE.get(a2, a2)
        result["call_put"] = CALL_PUT.get(a3, a3)
        result["exercise_style"] = EXERCISE_STYLE_EABM.get(a4, a4)

    elif group == "F":
        result["underlying_assets"] = WARRANT_UNDERLYING.get(a1, a1)
        result["barrier_dependency"] = MINI_BARRIER.get(a2, a2)
        result["direction"] = MINI_DIRECTION.get(a3, a3)
        result["exercise_style"] = EXERCISE_STYLE_EABM.get(a4, a4)

    elif group == "D":
        result["instrument_dependency"] = DEPOSITORY_DEPENDENCY.get(a1, a1)
        result["form"] = FORM.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given entitlements group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group == "A":
        return {"form": "Form"}
    if group in ("S", "P"):
        return {
            "assets": "Underlying Assets",
            "form": "Form",
        }
    if group == "W":
        return {
            "underlying_assets": "Underlying Assets",
            "type": "Type",
            "call_put": "Call/Put",
            "exercise_style": "Exercise Style",
        }
    if group == "F":
        return {
            "underlying_assets": "Underlying Assets",
            "barrier_dependency": "Barrier Dependency",
            "direction": "Long/Short",
            "exercise_style": "Exercise Style",
        }
    if group == "D":
        return {
            "instrument_dependency": "Instrument Dependency",
            "form": "Form",
        }
    return {}
