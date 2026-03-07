"""CFI Category H — Non-standard/exotic options (OTC options).

Attribute decoders and labels for all OTC-option groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import OPTION_STYLE_TYPE, VALUATION_METHOD


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class NonStandardGroup(Enum):
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
    "D": "Fixed-fixed swap",
    "E": "Interest rate index",
    "G": "Inflation rate swap",
    "H": "Overnight index swap",
    "O": "Options",
    "R": "Forwards",
    "F": "Futures",
    "M": "Others",
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
    "O": "Options",
    "R": "Forwards",
    "F": "Futures",
    "W": "Swaps",
    "M": "Others",
}

COMMODITY_VALUATION: Dict[str, str] = {
    "V": "Vanilla",
    "A": "Asian",
    "D": "Digital/binary",
    "B": "Barrier",
    "G": "Digital barrier",
    "L": "Lookback",
    "P": "Other path-dependent",
    "M": "Others",
}

COMMODITY_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "D": "Physical",
    "P": "Physical (commodity)",
    "N": "Non-deliverable",
}

EQUITY_UNDERLYING: Dict[str, str] = {
    "S": "Single stock",
    "I": "Index",
    "B": "Basket",
    "O": "Options",
    "R": "Forwards",
    "F": "Futures",
    "M": "Others",
}

EQUITY_VALUATION: Dict[str, str] = {
    "V": "Vanilla",
    "A": "Asian",
    "D": "Digital/binary",
    "B": "Barrier",
    "G": "Digital barrier",
    "L": "Lookback",
    "P": "Other path-dependent",
    "M": "Others",
}

EQUITY_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
    "E": "Elect at exercise",
}

CREDIT_UNDERLYING: Dict[str, str] = {
    "U": "CDS single name",
    "V": "CDS index tranche",
    "I": "CDS index",
    "W": "Swaps",
    "M": "Others",
}

FX_UNDERLYING: Dict[str, str] = {
    "R": "Forward — single",
    "F": "Futures — single",
    "T": "Spot — single",
    "V": "Volatility — single",
    "B": "Forward — index",
    "C": "Futures — index",
    "D": "Spot — index",
    "E": "Volatility — index",
    "Q": "Forward — basket",
    "U": "Futures — basket",
    "W": "Spot — basket",
    "Y": "Volatility — basket",
    "M": "Others",
}

FX_STYLE: Dict[str, str] = {
    "J": "European",
    "K": "American",
    "L": "Bermudan",
}

OTHER_UNDERLYING: Dict[str, str] = {
    "P": "Commercial property",
    "M": "Others",
}

OTHER_DELIVERY: Dict[str, str] = {
    "C": "Cash",
    "P": "Physical",
    "E": "Elect at exercise",
    "N": "Non-deliverable",
    "A": "Auction",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given non-standard option group code.

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
        result["option_style_type"] = OPTION_STYLE_TYPE.get(a2, a2)
        result["valuation_method"] = VALUATION_METHOD.get(a3, a3)
        result["delivery"] = EQUITY_DELIVERY.get(a4, a4)

    elif group == "T":
        result["underlying"] = COMMODITY_UNDERLYING.get(a1, a1)
        result["option_style_type"] = OPTION_STYLE_TYPE.get(a2, a2)
        result["valuation_method"] = COMMODITY_VALUATION.get(a3, a3)
        result["delivery"] = COMMODITY_DELIVERY.get(a4, a4)

    elif group == "E":
        result["underlying"] = EQUITY_UNDERLYING.get(a1, a1)
        result["option_style_type"] = OPTION_STYLE_TYPE.get(a2, a2)
        result["valuation_method"] = EQUITY_VALUATION.get(a3, a3)
        result["delivery"] = EQUITY_DELIVERY.get(a4, a4)

    elif group == "C":
        result["underlying"] = CREDIT_UNDERLYING.get(a1, a1)
        result["option_style_type"] = OPTION_STYLE_TYPE.get(a2, a2)
        result["valuation_method"] = EQUITY_VALUATION.get(a3, a3)
        result["delivery"] = EQUITY_DELIVERY.get(a4, a4)

    elif group == "F":
        result["underlying"] = FX_UNDERLYING.get(a1, a1)
        result["option_style_type"] = FX_STYLE.get(a2, a2)
        result["valuation_method"] = EQUITY_VALUATION.get(a3, a3)
        result["delivery"] = EQUITY_DELIVERY.get(a4, a4)

    elif group == "M":
        result["underlying"] = OTHER_UNDERLYING.get(a1, a1)
        result["option_style_type"] = OPTION_STYLE_TYPE.get(a2, a2)
        result["valuation_method"] = EQUITY_VALUATION.get(a3, a3)
        result["delivery"] = OTHER_DELIVERY.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given OTC-option group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    base = {
        "underlying": "Underlying",
        "option_style_type": "Option Style/Type",
        "valuation_method": "Valuation Method",
        "delivery": "Delivery",
    }
    if group in ("R", "T", "E", "C", "F", "M"):
        return base
    return {}
