"""CFI Category D — Debt instruments.

Attribute decoders and labels for all debt instrument groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import DEBT_GUARANTEE, DEBT_INTEREST_TYPE, DEBT_REDEMPTION, FORM


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class DebtGroup(Enum):
    BONDS = "B"
    CONVERTIBLE_BONDS = "C"
    BONDS_WITH_WARRANTS = "W"
    MEDIUM_TERM_NOTES = "T"
    MONEY_MARKET_INSTRUMENTS = "Y"
    STRUCTURED_INSTRUMENTS_CAPITAL_PROTECTION = "S"
    STRUCTURED_INSTRUMENTS_WITHOUT_CAPITAL_PROTECTION = "E"
    MORTGAGE_BACKED_SECURITIES = "G"
    ASSET_BACKED_SECURITIES = "A"
    MUNICIPAL_BONDS = "N"
    DEPOSITORY_RECEIPTS_ON_DEBT = "D"
    OTHERS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries (category-specific)
# ---------------------------------------------------------------------------

DEBT_INTEREST_MONEY_MARKET: Dict[str, str] = {
    "F": "Fixed rate",
    "Z": "Zero rate/discounted",
    "V": "Variable",
    "K": "Index linked",
}

DEBT_INTEREST_ABS: Dict[str, str] = {
    "F": "Fixed rate",
    "Z": "Zero rate/discounted",
    "V": "Variable",
}

STRUCTURED_CAPITAL_TYPE: Dict[str, str] = {
    "A": "Structured capital protection certificate",
    "B": "Capital protection note",
    "C": "Capital protection fund linked note",
    "D": "Convertible capital protection certificate",
    "M": "Others",
}

STRUCTURED_CAPITAL_DISTRIBUTION: Dict[str, str] = {
    "D": "Non-income",
    "F": "Fixed rate payments",
    "Y": "Variable payments",
    "V": "Optional payments",
    "M": "Others",
}

STRUCTURED_CAPITAL_REPAYMENT: Dict[str, str] = {
    "F": "Fixed investment amount",
    "V": "Variable",
    "M": "Others",
}

STRUCTURED_UNDERLYING: Dict[str, str] = {
    "B": "Baskets",
    "S": "Shares",
    "D": "Debt instruments",
    "T": "Commodities",
    "C": "Currencies",
    "I": "Indices",
    "N": "Interest rates",
    "M": "Others",
}

STRUCTURED_NOCAP_TYPE: Dict[str, str] = {
    "A": "Discount certificate",
    "B": "Bonus certificate",
    "C": "Outperformance certificate",
    "D": "Double outperformance certificate",
    "E": "Barrier reverse convertible",
    "M": "Others",
}

STRUCTURED_NOCAP_DISTRIBUTION: Dict[str, str] = {
    "D": "Non-income",
    "Y": "Annual",
    "M": "Others",
}

STRUCTURED_NOCAP_REPAYMENT: Dict[str, str] = {
    "R": "Fixed redemption above par",
    "S": "Fixed redemption at par",
    "C": "Conditional capital protection",
    "T": "Capital at risk",
    "M": "Others",
}

DEPOSITORY_DEPENDENCY: Dict[str, str] = {
    "C": "Convertible bonds",
    "W": "Bonds with warrants attached",
    "T": "Medium-term notes",
    "Y": "Money market instruments",
    "G": "Mortgage-backed securities",
    "A": "Asset-backed securities",
    "B": "Bonds",
    "N": "Municipal bonds",
    "M": "Others",
}

DEPOSITORY_INTEREST: Dict[str, str] = {
    "F": "Fixed rate",
    "Z": "Zero rate/discounted",
    "V": "Variable",
    "C": "Index linked",
}

OTHERS_TYPE: Dict[str, str] = {
    "B": "Bank loans",
    "P": "Promissory notes",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given debt group code.

    Args:
        group: Single-character group code (e.g. 'B', 'C').
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group in ("B", "C", "W", "T"):
        result["interest_type"] = DEBT_INTEREST_TYPE.get(a1, a1)
        result["guarantee_ranking"] = DEBT_GUARANTEE.get(a2, a2)
        result["redemption"] = DEBT_REDEMPTION.get(a3, a3)
        result["form"] = FORM.get(a4, a4)

    elif group == "Y":
        result["interest_type"] = DEBT_INTEREST_MONEY_MARKET.get(a1, a1)
        result["guarantee_ranking"] = DEBT_GUARANTEE.get(a2, a2)
        result["form"] = FORM.get(a4, a4)

    elif group == "S":
        result["type"] = STRUCTURED_CAPITAL_TYPE.get(a1, a1)
        result["distribution"] = STRUCTURED_CAPITAL_DISTRIBUTION.get(a2, a2)
        result["repayment"] = STRUCTURED_CAPITAL_REPAYMENT.get(a3, a3)
        result["underlying_assets"] = STRUCTURED_UNDERLYING.get(a4, a4)

    elif group == "E":
        result["type"] = STRUCTURED_NOCAP_TYPE.get(a1, a1)
        result["distribution"] = STRUCTURED_NOCAP_DISTRIBUTION.get(a2, a2)
        result["repayment"] = STRUCTURED_NOCAP_REPAYMENT.get(a3, a3)
        result["underlying_assets"] = STRUCTURED_UNDERLYING.get(a4, a4)

    elif group in ("G", "A", "N"):
        result["interest_type"] = DEBT_INTEREST_ABS.get(a1, a1)
        result["guarantee_ranking"] = DEBT_GUARANTEE.get(a2, a2)
        result["redemption"] = DEBT_REDEMPTION.get(a3, a3)
        result["form"] = FORM.get(a4, a4)

    elif group == "D":
        result["instrument_dependency"] = DEPOSITORY_DEPENDENCY.get(a1, a1)
        result["interest_type"] = DEPOSITORY_INTEREST.get(a2, a2)
        result["guarantee_ranking"] = DEBT_GUARANTEE.get(a3, a3)
        result["redemption"] = DEBT_REDEMPTION.get(a4, a4)

    elif group == "M":
        result["type"] = OTHERS_TYPE.get(a1, a1)
        result["form"] = FORM.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given debt group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group in ("B", "C", "W", "T"):
        return {
            "interest_type": "Type of Interest",
            "guarantee_ranking": "Guarantee/Ranking",
            "redemption": "Redemption",
            "form": "Form",
        }
    if group == "Y":
        return {
            "interest_type": "Type of Interest",
            "guarantee_ranking": "Guarantee/Ranking",
            "form": "Form",
        }
    if group == "S":
        return {
            "type": "Type",
            "distribution": "Distribution",
            "repayment": "Repayment",
            "underlying_assets": "Underlying Assets",
        }
    if group == "E":
        return {
            "type": "Type",
            "distribution": "Distribution",
            "repayment": "Repayment",
            "underlying_assets": "Underlying Assets",
        }
    if group in ("G", "A", "N"):
        return {
            "interest_type": "Type of Interest",
            "guarantee_ranking": "Guarantee/Ranking",
            "redemption": "Redemption",
            "form": "Form",
        }
    if group == "D":
        return {
            "instrument_dependency": "Instrument Dependency",
            "interest_type": "Type of Interest",
            "guarantee_ranking": "Guarantee/Ranking",
            "redemption": "Redemption",
        }
    if group == "M":
        return {
            "type": "Type",
            "form": "Form",
        }
    return {}
