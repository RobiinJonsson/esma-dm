"""CFI Category T — Referential instruments.

Attribute decoders and labels for all referential groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class ReferentialGroup(Enum):
    CURRENCIES = "C"
    COMMODITIES = "T"
    INTEREST_RATES = "R"
    INDICES = "I"
    BASKETS = "B"
    STOCK_DIVIDENDS = "D"
    OTHERS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

CURRENCY_TYPE: Dict[str, str] = {
    "N": "National currency",
    "L": "Legacy currency",
    "C": "Bullion coins",
    "M": "Others",
}

COMMODITY_TYPE: Dict[str, str] = {
    "E": "Extraction resources",
    "A": "Agriculture",
    "I": "Industrial products",
    "S": "Services",
    "N": "Environmental",
    "P": "Polypropylene products",
    "H": "Generated resources",
    "M": "Others",
}

IR_TYPE: Dict[str, str] = {
    "N": "Nominal",
    "V": "Variable",
    "F": "Fixed",
    "R": "Real",
    "M": "Others",
}

IR_FREQUENCY: Dict[str, str] = {
    "D": "Daily",
    "W": "Weekly",
    "N": "Monthly",
    "Q": "Quarterly",
    "S": "Semi-annually",
    "A": "Annually",
    "M": "Others",
}

INDEX_ASSET_CLASS: Dict[str, str] = {
    "E": "Equities",
    "D": "Debt instruments",
    "F": "CIVs",
    "R": "Real estate",
    "T": "Commodities",
    "C": "Currencies",
    "M": "Others",
}

INDEX_WEIGHTING: Dict[str, str] = {
    "P": "Price-weighted",
    "C": "Market capitalisation-weighted",
    "E": "Equal-weighted",
    "F": "Modified market capitalisation-weighted",
    "M": "Others",
}

INDEX_RETURN: Dict[str, str] = {
    "P": "Price return",
    "N": "Net total return",
    "G": "Gross total return",
    "M": "Others",
}

BASKET_COMPOSITION: Dict[str, str] = {
    "E": "Equities",
    "D": "Debt instruments",
    "F": "CIVs",
    "I": "Interest rates/IRDs",
    "T": "Commodities",
    "C": "Currencies",
    "M": "Others",
}

EQUITY_TYPE: Dict[str, str] = {
    "S": "Common/ordinary shares",
    "P": "Preferred/preference shares",
    "C": "Common/ordinary convertible shares",
    "F": "Preferred/preference convertible shares",
    "L": "Limited partnership units",
    "K": "CIVs",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given referential group code.

    Args:
        group: Single-character group code.
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "C":
        result["type"] = CURRENCY_TYPE.get(a1, a1)

    elif group == "T":
        result["type"] = COMMODITY_TYPE.get(a1, a1)

    elif group == "R":
        result["type"] = IR_TYPE.get(a1, a1)
        result["frequency"] = IR_FREQUENCY.get(a2, a2)

    elif group == "I":
        result["asset_classes"] = INDEX_ASSET_CLASS.get(a1, a1)
        result["weighting"] = INDEX_WEIGHTING.get(a2, a2)
        result["return_type"] = INDEX_RETURN.get(a3, a3)

    elif group == "B":
        result["composition"] = BASKET_COMPOSITION.get(a1, a1)

    elif group == "D":
        result["type_of_equity"] = EQUITY_TYPE.get(a1, a1)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given referential group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group == "C":
        return {"type": "Type"}
    if group == "T":
        return {"type": "Type"}
    if group == "R":
        return {
            "type": "Type",
            "frequency": "Frequency",
        }
    if group == "I":
        return {
            "asset_classes": "Asset Classes",
            "weighting": "Weighting",
            "return_type": "Return Type",
        }
    if group == "B":
        return {"composition": "Composition"}
    if group == "D":
        return {"type_of_equity": "Type of Equity"}
    return {}
