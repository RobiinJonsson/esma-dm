"""CFI package — ISO 10962 Classification of Financial Instruments.

Public API re-exports all enums, the CFI dataclass, and the top-level
decode/label/description helpers.

Usage::

    from esma_dm.models.utils.cfi import decode_cfi, CFI, Category, EquityGroup

    result = decode_cfi("ESVUFR")
    print(result)
"""

from .category import Category
from .cfi_instrument_manager import (
    CFI,
    CFIInstrumentTypeManager,
    decode_cfi,
    filter_firds_files_by_cfi,
    filter_fitrs_files_by_cfi,
    get_attribute_labels,
    get_firds_letter_for_type,
    get_firds_patterns_for_cfi,
    get_fitrs_patterns_for_cfi,
    get_instrument_type_from_cfi,
    get_instrument_type_from_firds_file,
    get_valid_instrument_types,
    group_description,
    normalize_instrument_type_from_cfi,
    validate_cfi_code,
    validate_instrument_type,
)

# Category-specific group enums
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

__all__ = [
    # Core decode helpers
    "CFI",
    "decode_cfi",
    "get_attribute_labels",
    "group_description",
    # File routing and type management
    "CFIInstrumentTypeManager",
    "filter_firds_files_by_cfi",
    "filter_fitrs_files_by_cfi",
    "get_firds_letter_for_type",
    "get_firds_patterns_for_cfi",
    "get_fitrs_patterns_for_cfi",
    "get_instrument_type_from_cfi",
    "get_instrument_type_from_firds_file",
    "get_valid_instrument_types",
    "normalize_instrument_type_from_cfi",
    "validate_cfi_code",
    "validate_instrument_type",
    # Category enum
    "Category",
    # Group enums
    "EquityGroup",
    "DebtGroup",
    "CIVGroup",
    "EntitlementsGroup",
    "OptionsGroup",
    "FuturesGroup",
    "SwapsGroup",
    "NonStandardGroup",
    "SpotGroup",
    "ForwardsGroup",
    "StrategiesGroup",
    "FinancingGroup",
    "ReferentialGroup",
    "OthersGroup",
]
