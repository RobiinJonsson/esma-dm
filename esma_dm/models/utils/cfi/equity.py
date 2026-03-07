"""CFI Category E — Equities.

Attribute decoders and labels for all equity instrument groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import FORM


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class EquityGroup(Enum):
    COMMON_SHARES = "S"
    PREFERRED_SHARES = "P"
    COMMON_CONVERTIBLE_SHARES = "C"
    PREFERRED_CONVERTIBLE_SHARES = "F"
    LIMITED_PARTNERSHIP_UNITS = "L"
    DEPOSITORY_RECEIPTS = "D"
    STRUCTURED_INSTRUMENTS = "Y"
    OTHERS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries (category-specific)
# ---------------------------------------------------------------------------

VOTING_RIGHT: Dict[str, str] = {
    "V": "Voting",
    "N": "Non-voting",
    "R": "Restricted voting",
    "E": "Enhanced voting",
}

OWNERSHIP_RESTRICTIONS: Dict[str, str] = {
    "T": "Restrictions",
    "U": "Free (unrestricted)",
}

PAYMENT_STATUS: Dict[str, str] = {
    "O": "Nil paid",
    "P": "Partly paid",
    "F": "Fully paid",
}

REDEMPTION_E: Dict[str, str] = {
    "R": "Redeemable",
    "E": "Extendible",
    "T": "Redeemable/extendible",
    "G": "Exchangeable",
    "A": "Redeemable/exchangeable/extendible",
    "C": "Redeemable/exchangeable",
    "N": "Perpetual",
}

INCOME: Dict[str, str] = {
    "F": "Fixed rate",
    "C": "Cumulative fixed rate",
    "P": "Participating",
    "Q": "Cumulative participating",
    "A": "Adjustable/variable",
    "N": "Normal rate",
    "U": "Auction rate",
}

DEPOSITORY_DEPENDENCY: Dict[str, str] = {
    "S": "Common/ordinary shares",
    "P": "Preferred/preference shares",
    "C": "Common/ordinary convertible shares",
    "F": "Preferred/preference convertible shares",
    "L": "Limited partnership units",
    "M": "Others",
}

DEPOSITORY_REDEMPTION: Dict[str, str] = {
    "R": "Redeemable",
    "N": "Non-redeemable",
    "B": "Redeemable at premium",
    "D": "Redeemable at discount",
}

DEPOSITORY_INCOME: Dict[str, str] = {
    "F": "Fixed rate",
    "C": "Cumulative fixed rate",
    "P": "Participating",
    "Q": "Cumulative participating",
    "A": "Adjustable/variable",
    "N": "Normal rate",
    "U": "Auction rate",
    "D": "Non-income",
}

STRUCTURED_TYPE: Dict[str, str] = {
    "A": "Capital protection certificate",
    "B": "Discount certificate",
    "C": "Barrier capital protection certificate",
    "D": "Outperformance certificate",
    "E": "Bonus certificate",
    "M": "Others",
}

STRUCTURED_DISTRIBUTION: Dict[str, str] = {
    "D": "Dividend payments",
    "Y": "Annual",
    "M": "Others/none",
}

STRUCTURED_REPAYMENT: Dict[str, str] = {
    "F": "Fixed investment amount",
    "V": "Variable",
    "E": "Early repayment",
    "M": "Others",
}

STRUCTURED_UNDERLYING: Dict[str, str] = {
    "B": "Baskets",
    "S": "Shares",
    "D": "Debt instruments",
    "G": "Derivatives",
    "T": "Commodities",
    "C": "Currencies",
    "I": "Indices",
    "N": "Interest rates",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given equity group code.

    Args:
        group: Single-character group code (e.g. 'S', 'P').
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group in ("S", "C", "L"):
        result["voting_right"] = VOTING_RIGHT.get(a1, a1)
        result["ownership_restrictions"] = OWNERSHIP_RESTRICTIONS.get(a2, a2)
        result["payment_status"] = PAYMENT_STATUS.get(a3, a3)
        result["form"] = FORM.get(a4, a4)

    elif group in ("P", "F"):
        result["voting_right"] = VOTING_RIGHT.get(a1, a1)
        result["redemption"] = REDEMPTION_E.get(a2, a2)
        result["income"] = INCOME.get(a3, a3)
        result["form"] = FORM.get(a4, a4)

    elif group == "D":
        result["instrument_dependency"] = DEPOSITORY_DEPENDENCY.get(a1, a1)
        result["redemption"] = DEPOSITORY_REDEMPTION.get(a2, a2)
        result["income"] = DEPOSITORY_INCOME.get(a3, a3)
        result["form"] = FORM.get(a4, a4)

    elif group == "Y":
        result["type"] = STRUCTURED_TYPE.get(a1, a1)
        result["distribution"] = STRUCTURED_DISTRIBUTION.get(a2, a2)
        result["repayment"] = STRUCTURED_REPAYMENT.get(a3, a3)
        result["underlying_assets"] = STRUCTURED_UNDERLYING.get(a4, a4)

    elif group == "M":
        result["form"] = FORM.get(a4, a4)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given equity group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group in ("S", "C", "L"):
        return {
            "voting_right": "Voting Right",
            "ownership_restrictions": "Ownership/Transfer Restrictions",
            "payment_status": "Payment Status",
            "form": "Form",
        }
    if group in ("P", "F"):
        return {
            "voting_right": "Voting Right",
            "redemption": "Redemption",
            "income": "Income",
            "form": "Form",
        }
    if group == "D":
        return {
            "instrument_dependency": "Instrument Dependency",
            "redemption": "Redemption/Conversion",
            "income": "Income",
            "form": "Form",
        }
    if group == "Y":
        return {
            "type": "Type",
            "distribution": "Distribution",
            "repayment": "Repayment",
            "underlying_assets": "Underlying Assets",
        }
    if group == "M":
        return {"form": "Form"}
    return {}
