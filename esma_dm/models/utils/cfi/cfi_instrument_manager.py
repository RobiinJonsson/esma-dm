"""CFI instrument manager — central dispatch for ISO 10962 CFI decoding.

Provides the CFI dataclass, decode_cfi(), get_attribute_labels(), and
group_description() functions that dispatch to the appropriate category module.

Also provides CFIInstrumentTypeManager for FIRDS/FITRS file routing and
instrument type determination based on CFI categories.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .category import Category
from . import (
    collective,
    debt,
    entitlements,
    equity,
    financing,
    forwards,
    futures,
    non_standard,
    options,
    others,
    referential,
    spot,
    strategies,
    swaps,
)
from .collective import CIVGroup
from .debt import DebtGroup
from .entitlements import EntitlementsGroup
from .equity import EquityGroup
from .financing import FinancingGroup
from .forwards import ForwardsGroup
from .futures import FuturesGroup
from .non_standard import NonStandardGroup
from .options import OptionsGroup
from .others import OthersGroup
from .referential import ReferentialGroup
from .spot import SpotGroup
from .strategies import StrategiesGroup
from .swaps import SwapsGroup


# ---------------------------------------------------------------------------
# Internal mapping: category code → (module, group enum class)
# ---------------------------------------------------------------------------

_CATEGORY_MAP = {
    "E": (equity, EquityGroup),
    "D": (debt, DebtGroup),
    "C": (collective, CIVGroup),
    "R": (entitlements, EntitlementsGroup),
    "O": (options, OptionsGroup),
    "F": (futures, FuturesGroup),
    "S": (swaps, SwapsGroup),
    "H": (non_standard, NonStandardGroup),
    "I": (spot, SpotGroup),
    "J": (forwards, ForwardsGroup),
    "K": (strategies, StrategiesGroup),
    "L": (financing, FinancingGroup),
    "T": (referential, ReferentialGroup),
    "M": (others, OthersGroup),
}


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class CFI:
    """Decoded representation of an ISO 10962 CFI code.

    Attributes:
        code: The original six-character CFI code.
        category: Human-readable category name.
        category_code: Single-character category code.
        group: Human-readable group name.
        group_code: Single-character group code.
        attributes: Dictionary of decoded attribute names to values.
    """

    code: str
    category: str
    category_code: str
    group: str
    group_code: str
    attributes: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        parts = [f"CFI({self.code})", f"  Category : {self.category}", f"  Group    : {self.group}"]
        for key, value in self.attributes.items():
            label = key.replace("_", " ").title()
            parts.append(f"  {label:<20}: {value}")
        return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def decode_cfi(cfi_code: str) -> Optional[CFI]:
    """Decode a six-character ISO 10962 CFI code into a CFI dataclass.

    Args:
        cfi_code: Six-character CFI code (case-insensitive).

    Returns:
        CFI dataclass with decoded fields, or None if the code is invalid.
    """
    if not isinstance(cfi_code, str) or len(cfi_code) != 6:
        return None

    code = cfi_code.upper()
    category_code = code[0]
    group_code = code[1]
    attr_str = code[2:]

    entry = _CATEGORY_MAP.get(category_code)
    if entry is None:
        return None

    module, group_enum_cls = entry

    # Resolve category name
    try:
        cat = Category(category_code)
        category_name = cat.name.replace("_", " ").title()
    except ValueError:
        category_name = category_code

    # Resolve group name
    try:
        grp = group_enum_cls(group_code)
        group_name = grp.name.replace("_", " ").title()
    except ValueError:
        group_name = group_code

    # Decode attributes
    try:
        attrs = module.decode_attributes(group_code, attr_str)
    except Exception:  # noqa: BLE001
        attrs = {}

    return CFI(
        code=code,
        category=category_name,
        category_code=category_code,
        group=group_name,
        group_code=group_code,
        attributes=attrs,
    )


def get_attribute_labels(cfi_code: str) -> Dict[str, str]:
    """Return human-readable display labels for each attribute key of a CFI code.

    Args:
        cfi_code: Six-character CFI code (case-insensitive).

    Returns:
        Dictionary mapping attribute key names to display labels.
        Returns an empty dict if the code is unrecognised.
    """
    if not isinstance(cfi_code, str) or len(cfi_code) != 6:
        return {}

    code = cfi_code.upper()
    category_code = code[0]
    group_code = code[1]

    entry = _CATEGORY_MAP.get(category_code)
    if entry is None:
        return {}

    module, _ = entry
    try:
        return module.attribute_labels(group_code)
    except Exception:  # noqa: BLE001
        return {}


def group_description(category_code: str, group_code: str) -> str:
    """Return the human-readable group name for a category/group code pair.

    Args:
        category_code: Single-character category code.
        group_code: Single-character group code.

    Returns:
        Human-readable group name, or the raw group_code if unrecognised.
    """
    entry = _CATEGORY_MAP.get(category_code.upper())
    if entry is None:
        return group_code

    _, group_enum_cls = entry
    try:
        grp = group_enum_cls(group_code.upper())
        return grp.name.replace("_", " ").title()
    except ValueError:
        return group_code


# ---------------------------------------------------------------------------
# CFIInstrumentTypeManager
# ---------------------------------------------------------------------------

class CFIInstrumentTypeManager:
    """Manages instrument type determination using CFI codes as the single source of truth.

    Maps FIRDS/FITRS file letters to CFI categories (ISO 10962) and provides
    helpers for file routing, business type derivation, and code validation.
    """

    # FIRDS file letter → CFI category
    FIRDS_TO_CFI_MAPPING: Dict[str, Category] = {
        "C": Category.COLLECTIVE_INVESTMENT,
        "D": Category.DEBT,
        "E": Category.EQUITIES,
        "F": Category.FUTURES,
        "H": Category.NON_STANDARD,
        "I": Category.SPOT,
        "J": Category.FORWARDS,
        "O": Category.OPTIONS,
        "R": Category.ENTITLEMENTS,
        "S": Category.SWAPS,
    }

    # CFI category → FITRS filename prefix/letter patterns
    CFI_TO_FITRS_MAPPING: Dict[Category, List[str]] = {
        Category.COLLECTIVE_INVESTMENT: ["FULNCR_", "_C_", "FULECR_"],
        Category.DEBT: ["FULNCR_", "_D_"],
        Category.EQUITIES: ["FULECR_", "_E_", "FULNCR_"],
        Category.FUTURES: ["FULNCR_", "_F_"],
        Category.NON_STANDARD: ["FULNCR_", "_H_"],
        Category.SPOT: ["FULNCR_", "_I_"],
        Category.FORWARDS: ["FULNCR_", "_J_"],
        Category.OPTIONS: ["FULNCR_", "_O_"],
        Category.ENTITLEMENTS: ["FULECR_", "_R_"],
        Category.SWAPS: ["FULNCR_", "_S_"],
    }

    # CFI category → business type string
    CFI_TO_BUSINESS_TYPE: Dict[Category, str] = {
        Category.COLLECTIVE_INVESTMENT: "collective_investment",
        Category.DEBT: "debt",
        Category.EQUITIES: "equity",
        Category.FUTURES: "future",
        Category.NON_STANDARD: "structured",
        Category.SPOT: "spot",
        Category.FORWARDS: "forward",
        Category.OPTIONS: "option",
        Category.ENTITLEMENTS: "rights",
        Category.SWAPS: "swap",
        Category.STRATEGIES: "strategy",
        Category.FINANCING: "financing",
        Category.REFERENTIAL: "referential",
        Category.OTHERS: "other",
    }

    # CFI category → FITRS letter list
    _FITRS_LETTER_MAP: Dict[Category, List[str]] = {
        Category.COLLECTIVE_INVESTMENT: ["C"],
        Category.DEBT: ["D"],
        Category.EQUITIES: ["E"],
        Category.FUTURES: ["F"],
        Category.NON_STANDARD: ["H"],
        Category.SPOT: ["I"],
        Category.FORWARDS: ["J"],
        Category.OPTIONS: ["O"],
        Category.ENTITLEMENTS: ["R"],
        Category.SWAPS: ["S"],
    }

    @classmethod
    def determine_cfi_from_firds_file(cls, firds_file_letter: str) -> Optional[Category]:
        """Return the CFI Category for a FIRDS file letter, or None if unrecognised."""
        return cls.FIRDS_TO_CFI_MAPPING.get(firds_file_letter.upper())

    @classmethod
    def get_business_type_from_cfi(cls, cfi_code: str) -> str:
        """Return the business instrument type string for a CFI code."""
        if not cfi_code:
            return "other"
        try:
            category = Category(cfi_code[0].upper())
            return cls.CFI_TO_BUSINESS_TYPE.get(category, "other")
        except ValueError:
            return "other"

    @classmethod
    def get_business_type_from_firds_file(cls, firds_file_letter: str) -> str:
        """Return the business instrument type string for a FIRDS file letter."""
        category = cls.determine_cfi_from_firds_file(firds_file_letter)
        return cls.CFI_TO_BUSINESS_TYPE.get(category, "other") if category else "other"

    @classmethod
    def get_fitrs_patterns_from_cfi(cls, cfi_code: str) -> List[str]:
        """Return FITRS file letter list for a CFI code (e.g. ['E'])."""
        if not cfi_code:
            return [cat.value for cat in cls._FITRS_LETTER_MAP]
        try:
            category = Category(cfi_code[0].upper())
            return cls._FITRS_LETTER_MAP.get(category, ["C"])
        except ValueError:
            return ["C"]

    @classmethod
    def get_firds_patterns_from_cfi(cls, cfi_code: str) -> List[str]:
        """Return FIRDS file letter list for a CFI code (e.g. ['E'])."""
        return cls.get_fitrs_patterns_from_cfi(cfi_code)

    @classmethod
    def filter_firds_files_by_cfi(cls, all_files: List[str], cfi_code: str) -> List[str]:
        """Filter a list of FIRDS filenames to those matching the CFI code's category."""
        target_letters = cls.get_firds_patterns_from_cfi(cfi_code)
        result = []
        for filename in all_files:
            m = re.match(r"^FULINS_([A-Z])_\d{8}_\d+of\d+_firds_data\.csv$", filename)
            if m and m.group(1) in target_letters:
                result.append(filename)
        return result

    @classmethod
    def filter_fitrs_files_by_cfi(cls, all_files: List[str], cfi_code: str) -> List[str]:
        """Filter a list of FITRS filenames to those matching the CFI code's category."""
        target_letters = cls.get_fitrs_patterns_from_cfi(cfi_code)
        result = []
        for filename in all_files:
            m = re.match(r"^FUL(ECR|NCR)_\d{8}_([A-Z])_\d+of\d+_fitrs_data\.csv$", filename)
            if not m:
                continue
            file_type, file_letter = m.group(1), m.group(2)
            if file_letter not in target_letters:
                continue
            try:
                cfi_category = Category(cfi_code[0].upper())
            except ValueError:
                continue
            if cfi_category == Category.EQUITIES:
                result.append(filename)
            elif cfi_category == Category.ENTITLEMENTS and file_type == "ECR":
                result.append(filename)
            elif cfi_category == Category.COLLECTIVE_INVESTMENT:
                result.append(filename)
            elif file_type == "NCR":
                result.append(filename)
        return result

    @classmethod
    def validate_instrument_type(cls, instrument_type: str) -> bool:
        """Return True if the instrument type string is recognised."""
        return instrument_type.lower() in cls.CFI_TO_BUSINESS_TYPE.values()

    @classmethod
    def validate_cfi_code(cls, cfi_code: str) -> Tuple[bool, str]:
        """Return (is_valid, error_message) for a CFI code string."""
        if not cfi_code:
            return False, "CFI code cannot be empty"
        if len(cfi_code) != 6:
            return False, f"CFI code must be 6 characters, got {len(cfi_code)}"
        if cfi_code[0].upper() not in [cat.value for cat in Category]:
            return False, f"Unsupported CFI category: {cfi_code[0].upper()}"
        return True, ""

    @classmethod
    def get_valid_instrument_types(cls) -> List[str]:
        """Return all recognised business instrument type strings."""
        return list(set(cls.CFI_TO_BUSINESS_TYPE.values()))

    @classmethod
    def normalize_instrument_type_from_cfi(cls, cfi_code: str) -> str:
        """Return the normalised instrument type for a CFI code."""
        return cls.get_business_type_from_cfi(cfi_code)

    @classmethod
    def validate_cfi_consistency(cls, cfi_code: str, firds_file_letter: str) -> Tuple[bool, str]:
        """Return (is_consistent, error_message) checking CFI vs FIRDS file letter."""
        if not cfi_code:
            return False, "Invalid CFI code"
        expected = cls.determine_cfi_from_firds_file(firds_file_letter)
        if not expected:
            return False, f"Invalid FIRDS file letter: {firds_file_letter}"
        actual = cfi_code[0].upper()
        if actual != expected.value:
            return False, (
                f"CFI category '{actual}' does not match FIRDS file "
                f"'{firds_file_letter}' (expected '{expected.value}')"
            )
        return True, ""

    @classmethod
    def create_cfi_from_firds_context(
        cls, firds_file_letter: str, group: str = "S", attributes: str = "XXXX"
    ) -> str:
        """Construct a CFI code string from a FIRDS file letter and optional group/attributes."""
        category = cls.determine_cfi_from_firds_file(firds_file_letter)
        prefix = category.value if category else "M"
        return f"{prefix}{group}{attributes}"

    @classmethod
    def get_cfi_info(cls, cfi_code: str) -> Dict[str, Any]:
        """Return a summary dict for a CFI code including decoded attributes and business type."""
        decoded = decode_cfi(cfi_code)
        business_type = cls.get_business_type_from_cfi(cfi_code)
        fitrs_patterns = cls.get_fitrs_patterns_from_cfi(cfi_code)
        if decoded is None:
            return {
                "error": f"Unrecognised CFI code: {cfi_code}",
                "cfi_code": cfi_code,
                "business_type": "other",
                "fitrs_patterns": fitrs_patterns,
            }
        return {
            "cfi_code": decoded.code,
            "category": decoded.category,
            "category_code": decoded.category_code,
            "group": decoded.group,
            "group_code": decoded.group_code,
            "attributes": decoded.attributes,
            "business_type": business_type,
            "fitrs_patterns": fitrs_patterns,
        }


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

def get_instrument_type_from_cfi(cfi_code: str) -> str:
    """Return the business instrument type for a CFI code."""
    return CFIInstrumentTypeManager.get_business_type_from_cfi(cfi_code)


def get_instrument_type_from_firds_file(firds_file_letter: str) -> str:
    """Return the business instrument type for a FIRDS file letter."""
    return CFIInstrumentTypeManager.get_business_type_from_firds_file(firds_file_letter)


def get_fitrs_patterns_for_cfi(cfi_code: str) -> List[str]:
    """Return FITRS file letter list for a CFI code."""
    return CFIInstrumentTypeManager.get_fitrs_patterns_from_cfi(cfi_code)


def get_firds_patterns_for_cfi(cfi_code: str) -> List[str]:
    """Return FIRDS file letter list for a CFI code."""
    return CFIInstrumentTypeManager.get_firds_patterns_from_cfi(cfi_code)


def filter_firds_files_by_cfi(all_files: List[str], cfi_code: str) -> List[str]:
    """Filter FIRDS filenames by CFI code category."""
    return CFIInstrumentTypeManager.filter_firds_files_by_cfi(all_files, cfi_code)


def filter_fitrs_files_by_cfi(all_files: List[str], cfi_code: str) -> List[str]:
    """Filter FITRS filenames by CFI code category."""
    return CFIInstrumentTypeManager.filter_fitrs_files_by_cfi(all_files, cfi_code)


def validate_instrument_type(instrument_type: str) -> bool:
    """Return True if the instrument type string is recognised."""
    return CFIInstrumentTypeManager.validate_instrument_type(instrument_type)


def validate_cfi_code(cfi_code: str) -> Tuple[bool, str]:
    """Return (is_valid, error_message) for a CFI code."""
    return CFIInstrumentTypeManager.validate_cfi_code(cfi_code)


def get_valid_instrument_types() -> List[str]:
    """Return all recognised business instrument type strings."""
    return CFIInstrumentTypeManager.get_valid_instrument_types()


def normalize_instrument_type_from_cfi(cfi_code: str) -> str:
    """Return the normalised instrument type for a CFI code."""
    return CFIInstrumentTypeManager.normalize_instrument_type_from_cfi(cfi_code)


def get_firds_letter_for_type(instrument_type: str) -> Optional[str]:
    """Return the FIRDS file letter for a given business instrument type string."""
    business_to_cfi = {v: k for k, v in CFIInstrumentTypeManager.CFI_TO_BUSINESS_TYPE.items()}
    cfi_category = business_to_cfi.get(instrument_type.lower())
    if not cfi_category:
        return None
    for letter, category in CFIInstrumentTypeManager.FIRDS_TO_CFI_MAPPING.items():
        if category == cfi_category:
            return letter
    return None
