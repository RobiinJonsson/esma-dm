"""CFI Category S — Swaps.

Attribute decoders and labels for all swap groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class SwapsGroup(Enum):
    RATES = "R"
    COMMODITIES = "T"
    EQUITY = "E"
    CREDIT = "C"
    FOREIGN_EXCHANGE = "F"
    OTHERS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

RATES_UNDERLYING: Dict[str, str] = {
    "A": "Basis swap",
    "C": "Fixed-float swap",
    "D": "Fixed-fixed cross-currency swap",
    "G": "Inflation rate swap",
    "H": "Overnight index swap",
    "Z": "Zero coupon swap",
    "M": "Others",
}

RATES_NOTIONAL: Dict[str, str] = {
    "C": "Constant",
    "I": "Accreting",
    "D": "Amortizing",
    "Y": "Custom",
}

RATES_CURRENCY: Dict[str, str] = {
    "S": "Single currency",
    "C": "Cross-currency",
}

RATES_DELIVERY: Dict[str, str] = {
    "D": "Cash",
    "N": "Non-deliverable",
}

COMMODITY_UNDERLYING: Dict[str, str] = {
    "J": "Energy",
    "K": "Metals",
    "A": "Agriculture",
    "N": "Environmental",
    "G": "Freight",
    "P": "Polypropylene products",
    "S": "Fertilizer",
    "T": "Paper",
    "B": "Basket — single commodity",
    "C": "Basket — multiple commodities",
    "H": "Index — multiple commodities",
    "I": "Index — single commodity",
    "Q": "Multi-commodity",
    "M": "Others",
}

COMMODITY_RETURN: Dict[str, str] = {
    "C": "Contract for difference (CFD)",
    "T": "Total return",
}

COMMODITY_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
    "E": "Elect at exercise",
}

EQUITY_UNDERLYING: Dict[str, str] = {
    "S": "Single stock",
    "I": "Index",
    "B": "Basket",
    "M": "Others",
}

EQUITY_RETURN: Dict[str, str] = {
    "P": "Price return",
    "D": "Dividend return",
    "V": "Variance return",
    "L": "Volatility return",
    "T": "Total return",
    "C": "Contract for difference (CFD)",
    "M": "Others",
}

CREDIT_UNDERLYING: Dict[str, str] = {
    "U": "Single name CDS",
    "V": "Index tranche CDS",
    "I": "Index CDS",
    "B": "Basket",
    "M": "Others",
}

CREDIT_RETURN: Dict[str, str] = {
    "C": "Credit default",
    "T": "Total return",
    "M": "Others",
}

CREDIT_ISSUER: Dict[str, str] = {
    "C": "Corporate",
    "S": "Sovereign",
    "L": "Local",
}

CREDIT_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "D": "Physical",
    "A": "Auction",
}

FX_UNDERLYING: Dict[str, str] = {
    "A": "Spot-forward swap",
    "C": "Forward-forward swap",
    "M": "Others",
}

FX_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
}

OTHER_UNDERLYING: Dict[str, str] = {
    "P": "Commercial property",
    "M": "Others",
}

OTHER_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
    "E": "Elect at exercise",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given swap group code.

    Args:
        group: Single-character group code.
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "R":
        result["underlying"] = RATES_UNDERLYING.get(a1, a1)
        result["notional"] = RATES_NOTIONAL.get(a2, a2)
        result["currency"] = RATES_CURRENCY.get(a3, a3)
        result["delivery"] = RATES_DELIVERY.get(a4, a4)

    elif group == "T":
        result["underlying"] = COMMODITY_UNDERLYING.get(a1, a1)
        result["return_trigger"] = COMMODITY_RETURN.get(a2, a2)
        result["delivery"] = COMMODITY_DELIVERY.get(a4, a4)

    elif group == "E":
        result["underlying"] = EQUITY_UNDERLYING.get(a1, a1)
        result["return_trigger"] = EQUITY_RETURN.get(a2, a2)
        result["delivery"] = COMMODITY_DELIVERY.get(a4, a4)

    elif group == "C":
        result["underlying"] = CREDIT_UNDERLYING.get(a1, a1)
        result["return_trigger"] = CREDIT_RETURN.get(a2, a2)
        result["issuer_type"] = CREDIT_ISSUER.get(a3, a3)
        result["delivery"] = CREDIT_DELIVERY.get(a4, a4)

    elif group == "F":
        result["underlying"] = FX_UNDERLYING.get(a1, a1)
        result["delivery"] = FX_DELIVERY.get(a4, a4)

    elif group == "M":
        result["underlying"] = OTHER_UNDERLYING.get(a1, a1)
        result["delivery"] = OTHER_DELIVERY.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given swap group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group == "R":
        return {
            "underlying": "Underlying",
            "notional": "Notional Schedule",
            "currency": "Single/Multi-currency",
            "delivery": "Delivery",
        }
    if group == "T":
        return {
            "underlying": "Underlying",
            "return_trigger": "Return/Trigger",
            "delivery": "Delivery",
        }
    if group == "E":
        return {
            "underlying": "Underlying",
            "return_trigger": "Return/Trigger",
            "delivery": "Delivery",
        }
    if group == "C":
        return {
            "underlying": "Underlying",
            "return_trigger": "Return/Trigger",
            "issuer_type": "Issuer Type",
            "delivery": "Delivery",
        }
    if group == "F":
        return {
            "underlying": "Underlying",
            "delivery": "Delivery",
        }
    if group == "M":
        return {
            "underlying": "Underlying",
            "delivery": "Delivery",
        }
    return {}
