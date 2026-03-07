"""CFI Category C — Collective investment vehicles (CIV).

Attribute decoders and labels for all CIV groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class CIVGroup(Enum):
    STANDARD_FUNDS = "I"
    HEDGE_FUNDS = "H"
    REAL_ESTATE_INVESTMENT_TRUSTS = "B"
    ETF = "E"
    PENSION_FUNDS = "S"
    FUNDS_OF_FUNDS = "F"
    PRIVATE_EQUITY = "P"
    OTHERS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

CLOSED_OPEN: Dict[str, str] = {
    "C": "Closed-ended",
    "O": "Open-ended",
    "M": "Others",
}

DISTRIBUTION_CIV: Dict[str, str] = {
    "I": "Income funds (dividends)",
    "G": "Growth funds (no distribution)",
    "J": "Mixed funds",
}

ASSETS: Dict[str, str] = {
    "R": "Real estate",
    "B": "Bonds/debt instruments",
    "E": "Equities",
    "V": "Convertible securities",
    "L": "Mixed — loans and bonds",
    "C": "Commodities",
    "D": "Derivatives",
    "F": "Foreign exchange",
    "K": "Money market instruments",
    "M": "Others/mixed",
}

SECURITY_TYPE: Dict[str, str] = {
    "S": "Shares",
    "Q": "Units",
    "U": "Unknown/other",
    "Y": "Depositary receipts",
}

HEDGE_STRATEGY: Dict[str, str] = {
    "D": "Directional",
    "R": "Relative value/arbitrage",
    "S": "Security selection",
    "E": "Event driven",
    "A": "Multi-strategy",
    "N": "Global macro",
    "L": "Long/short equity",
    "M": "Others",
}

PENSION_STRATEGY: Dict[str, str] = {
    "B": "Balanced",
    "G": "Growth",
    "L": "Long-term",
    "M": "Others",
}

PENSION_TYPE: Dict[str, str] = {
    "R": "Defined contribution",
    "B": "Defined benefit",
    "M": "Others",
}

PENSION_SECURITY: Dict[str, str] = {
    "S": "Shares",
    "U": "Unknown/other",
}

FUNDS_OF_FUNDS_TYPE: Dict[str, str] = {
    "I": "Standard/regulated funds",
    "H": "Hedge funds",
    "B": "Real estate investment trusts",
    "E": "ETF",
    "P": "Private equity",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given CIV group code.

    Args:
        group: Single-character group code.
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group in ("I", "E", "P"):
        result["closed_open"] = CLOSED_OPEN.get(a1, a1)
        result["distribution"] = DISTRIBUTION_CIV.get(a2, a2)
        result["assets"] = ASSETS.get(a3, a3)
        result["security_type"] = SECURITY_TYPE.get(a4, a4)

    elif group == "H":
        result["strategy"] = HEDGE_STRATEGY.get(a1, a1)

    elif group == "B":
        result["closed_open"] = CLOSED_OPEN.get(a1, a1)
        result["distribution"] = DISTRIBUTION_CIV.get(a2, a2)
        result["security_type"] = SECURITY_TYPE.get(a4, a4)

    elif group == "S":
        result["closed_open"] = CLOSED_OPEN.get(a1, a1)
        result["strategy"] = PENSION_STRATEGY.get(a2, a2)
        result["type_of_pension"] = PENSION_TYPE.get(a3, a3)
        result["security_type"] = PENSION_SECURITY.get(a4, a4)

    elif group == "F":
        result["closed_open"] = CLOSED_OPEN.get(a1, a1)
        result["distribution"] = DISTRIBUTION_CIV.get(a2, a2)
        result["types_of_funds"] = FUNDS_OF_FUNDS_TYPE.get(a3, a3)
        result["security_type"] = SECURITY_TYPE.get(a4, a4)

    elif group == "M":
        result["security_type"] = SECURITY_TYPE.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given CIV group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group in ("I", "E", "P"):
        return {
            "closed_open": "Closed/Open-ended",
            "distribution": "Distribution Policy",
            "assets": "Assets",
            "security_type": "Security Type",
        }
    if group == "H":
        return {"strategy": "Investment Strategy"}
    if group == "B":
        return {
            "closed_open": "Closed/Open-ended",
            "distribution": "Distribution Policy",
            "security_type": "Security Type",
        }
    if group == "S":
        return {
            "closed_open": "Closed/Open-ended",
            "strategy": "Investment Strategy",
            "type_of_pension": "Type of Pension",
            "security_type": "Security Type",
        }
    if group == "F":
        return {
            "closed_open": "Closed/Open-ended",
            "distribution": "Distribution Policy",
            "types_of_funds": "Types of Constituent Funds",
            "security_type": "Security Type",
        }
    if group == "M":
        return {"security_type": "Security Type"}
    return {}
