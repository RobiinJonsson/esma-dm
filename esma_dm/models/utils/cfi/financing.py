"""CFI Category L — Financing instruments.

Attribute decoders and labels for all financing groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import DELIVERY_REPO


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class FinancingGroup(Enum):
    LOAN_LEASE = "L"
    REPO = "R"
    SECURITIES_LENDING = "S"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

LOAN_UNDERLYING: Dict[str, str] = {
    "A": "Agriculture",
    "B": "Baskets",
    "J": "Energy",
    "K": "Metals",
    "N": "Environmental",
    "P": "Polypropylene products",
    "S": "Fertilizer",
    "T": "Paper",
    "M": "Others",
}

LOAN_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
}

REPO_COLLATERAL: Dict[str, str] = {
    "G": "General collateral",
    "S": "Specific security",
    "C": "Cash",
}

REPO_TERMINATION: Dict[str, str] = {
    "F": "Flexible",
    "N": "Overnight",
    "O": "Open",
    "T": "Term",
}

SL_UNDERLYING: Dict[str, str] = {
    "C": "Cash",
    "G": "Government bonds",
    "P": "Corporate bonds",
    "T": "Convertible bonds",
    "E": "Equity",
    "L": "Letter of credit",
    "D": "Certificate of deposit",
    "W": "Warrants",
    "K": "Money market instruments",
    "M": "Others",
}

SL_TERMINATION: Dict[str, str] = {
    "N": "Overnight",
    "O": "Open",
    "T": "Term",
}

SL_DELIVERY: Dict[str, str] = {
    "D": "Delivery versus payment",
    "F": "Free of payment",
    "H": "Hold in custody",
    "T": "Tri-party",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given financing group code.

    Args:
        group: Single-character group code ('L', 'R', or 'S').
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "L":
        result["underlying_assets"] = LOAN_UNDERLYING.get(a1, a1)
        result["delivery"] = LOAN_DELIVERY.get(a4, a4)

    elif group == "R":
        result["collateral"] = REPO_COLLATERAL.get(a1, a1)
        result["termination"] = REPO_TERMINATION.get(a2, a2)
        result["delivery"] = DELIVERY_REPO.get(a4, a4)

    elif group == "S":
        result["underlying_assets"] = SL_UNDERLYING.get(a1, a1)
        result["termination"] = SL_TERMINATION.get(a2, a2)
        result["delivery"] = SL_DELIVERY.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given financing group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group == "L":
        return {
            "underlying_assets": "Underlying Assets",
            "delivery": "Delivery",
        }
    if group == "R":
        return {
            "collateral": "Collateral",
            "termination": "Termination",
            "delivery": "Delivery",
        }
    if group == "S":
        return {
            "underlying_assets": "Underlying Assets",
            "termination": "Termination",
            "delivery": "Delivery",
        }
    return {}
