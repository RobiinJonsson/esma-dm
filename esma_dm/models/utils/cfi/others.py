"""CFI Category M — Others (miscellaneous instruments).

Attribute decoders and labels for all miscellaneous groups per ISO 10962.
"""

from enum import Enum
from typing import Any, Dict

from ._shared import FORM


# ---------------------------------------------------------------------------
# Group enum
# ---------------------------------------------------------------------------

class OthersGroup(Enum):
    COMBINED_INSTRUMENTS = "C"
    OTHER_ASSETS = "M"


# ---------------------------------------------------------------------------
# Attribute value dictionaries
# ---------------------------------------------------------------------------

COMBINED_COMPONENTS: Dict[str, str] = {
    "S": "Shares",
    "B": "Bonds",
    "H": "Share + bond",
    "A": "Share + warrant",
    "W": "Warrant + warrant",
    "U": "Fund units + other",
    "M": "Others",
}

OWNERSHIP_RESTRICTIONS: Dict[str, str] = {
    "T": "Restrictions",
    "U": "Free (unrestricted)",
}

OTHER_GROUPING: Dict[str, str] = {
    "R": "Real estate deeds",
    "I": "Insurance products",
    "E": "Escrow receipts",
    "T": "Trade finance instruments",
    "N": "Carbon credits",
    "P": "Precious metal receipts",
    "S": "OTC derivatives",
    "M": "Others",
}


# ---------------------------------------------------------------------------
# Decoder
# ---------------------------------------------------------------------------

def decode_attributes(group: str, attrs: str) -> Dict[str, Any]:
    """Decode attributes 1-4 for a given miscellaneous group code.

    Args:
        group: Single-character group code ('C' or 'M').
        attrs: Four-character attribute string (positions 3-6 of the CFI code).

    Returns:
        Dictionary mapping attribute names to decoded values.
    """
    a1, a2, a3, a4 = attrs[0], attrs[1], attrs[2], attrs[3]
    result: Dict[str, Any] = {}

    if group == "C":
        result["components"] = COMBINED_COMPONENTS.get(a1, a1)
        result["ownership_restrictions"] = OWNERSHIP_RESTRICTIONS.get(a2, a2)
        result["form"] = FORM.get(a4, a4)

    elif group == "M":
        result["further_grouping"] = OTHER_GROUPING.get(a1, a1)

    return result


def attribute_labels(group: str) -> Dict[str, str]:
    """Return human-readable labels for attribute keys of a given miscellaneous group.

    Args:
        group: Single-character group code.

    Returns:
        Dictionary mapping attribute key names to display labels.
    """
    if group == "C":
        return {
            "components": "Components",
            "ownership_restrictions": "Ownership/Transfer Restrictions",
            "form": "Form",
        }
    if group == "M":
        return {"further_grouping": "Further Grouping"}
    return {}
