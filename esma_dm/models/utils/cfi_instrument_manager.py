"""
CFI-based instrument type management
Ensures consistency across the entire project by using CFI codes as the single source of truth
"""

from typing import Dict, List, Optional, Tuple

from esma_dm.models.utils.cfi import CFI, Category


class CFIInstrumentTypeManager:
    """
    Manages instrument type determination using CFI codes as the single source of truth.

    This class ensures that:
    1. FIRDS file type letters directly map to CFI categories (ISO 10962)
    2. instrument_type is always derived from CFI codes, not hardcoded
    3. FITRS file patterns are determined by CFI categories
    4. No variations of instrument_type exist in local variables
    """

    # FIRDS file letter to CFI category mapping (ISO 10962 standard)
    FIRDS_TO_CFI_MAPPING = {
        "C": Category.COLLECTIVE_INVESTMENT,  # Collective Investment Vehicles
        "D": Category.DEBT,  # Debt instruments
        "E": Category.EQUITIES,  # Equities
        "F": Category.FUTURES,  # Futures
        "H": Category.NON_STANDARD,  # Non-standardized derivatives (Structured)
        "I": Category.SPOT,  # Spot (Index-linked)
        "J": Category.FORWARDS,  # Forwards (Warrants in FIRDS context)
        "O": Category.OPTIONS,  # Options
        "R": Category.ENTITLEMENTS,  # Entitlements (Rights)
        "S": Category.SWAPS,  # Swaps
    }

    # CFI category to FITRS file patterns mapping
    # FITRS files have format: FUL{ECR|NCR}_{date}_{letter}_{part}_fitrs_data.csv
    # We use precise patterns that match the specific letter position
    CFI_TO_FITRS_MAPPING = {
        Category.COLLECTIVE_INVESTMENT: [
            "FULNCR_",
            "_C_",
            "FULECR_",
        ],  # Both equity and non-equity CIVs
        Category.DEBT: ["FULNCR_", "_D_"],  # Debt instruments only in non-equity
        Category.EQUITIES: ["FULECR_", "_E_", "FULNCR_"],  # Both equity and non-equity versions
        Category.FUTURES: ["FULNCR_", "_F_"],  # Futures only in non-equity
        Category.NON_STANDARD: ["FULNCR_", "_H_"],  # Structured products only in non-equity
        Category.SPOT: ["FULNCR_", "_I_"],  # Index-linked only in non-equity
        Category.FORWARDS: ["FULNCR_", "_J_"],  # Warrants only in non-equity
        Category.OPTIONS: ["FULNCR_", "_O_"],  # Options only in non-equity
        Category.ENTITLEMENTS: ["FULECR_", "_R_"],  # Rights only in equity
        Category.SWAPS: ["FULNCR_", "_S_"],  # Swaps only in non-equity
    }

    @classmethod
    def _filter_fitrs_files(cls, all_files: List[str], cfi_category: Category) -> List[str]:
        """
        Filter FITRS files based on CFI category using precise matching.

        Args:
            all_files: List of all FITRS filenames
            cfi_category: CFI category to filter for

        Returns:
            List of matching filenames
        """
        import re

        pattern_pairs = cls.CFI_TO_FITRS_PATTERNS.get(cfi_category, [])
        matching_files = []

        for filename in all_files:
            for prefix, letter_suffix in pattern_pairs:
                # Extract the letter from the pattern (remove underscores)
                letter = letter_suffix.strip("_")

                # Create specific regex pattern for this file type
                # Pattern: FUL{ECR|NCR}_{8digits}_{letter}_{digits}of{digits}_fitrs_data.csv
                pattern = f"^{re.escape(prefix)}\\d{{8}}_{letter}_\\d+of\\d+_fitrs_data\\.csv$"

                if re.match(pattern, filename):
                    matching_files.append(filename)
                    break  # Don't add the same file multiple times

        return matching_files

    # CFI category to business type descriptions
    CFI_TO_BUSINESS_TYPE = {
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

    @classmethod
    def determine_cfi_from_firds_file(cls, firds_file_letter: str) -> Optional[Category]:
        """
        Determine CFI category from FIRDS file letter.

        Args:
            firds_file_letter: Single letter from FIRDS filename (C, D, E, F, H, I, J, O, R, S)

        Returns:
            CFI Category enum value or None if invalid
        """
        return cls.FIRDS_TO_CFI_MAPPING.get(firds_file_letter.upper())

    @classmethod
    def get_business_type_from_cfi(cls, cfi_code: str) -> str:
        """
        Get business instrument type from CFI code.

        Args:
            cfi_code: 6-character CFI code

        Returns:
            Business type string (e.g., 'equity', 'debt', 'structured')
        """
        if not cfi_code or len(cfi_code) < 1:
            return "other"

        try:
            category = Category(cfi_code[0].upper())
            return cls.CFI_TO_BUSINESS_TYPE.get(category, "other")
        except ValueError:
            return "other"

    @classmethod
    def get_business_type_from_firds_file(cls, firds_file_letter: str) -> str:
        """
        Get business instrument type from FIRDS file letter.

        Args:
            firds_file_letter: Single letter from FIRDS filename

        Returns:
            Business type string
        """
        category = cls.determine_cfi_from_firds_file(firds_file_letter)
        if category:
            return cls.CFI_TO_BUSINESS_TYPE.get(category, "other")
        return "other"

    @classmethod
    def get_fitrs_patterns_from_cfi(cls, cfi_code: str) -> List[str]:
        """
        Get FITRS file letter patterns from CFI code.

        Args:
            cfi_code: 6-character CFI code

        Returns:
            List of specific FITRS file letters to search for (e.g., ['E'], ['D'])
        """
        if not cfi_code or len(cfi_code) < 1:
            return ["C", "D", "E", "F", "H", "I", "J", "O", "R", "S"]  # Fallback to all letters

        try:
            category = Category(cfi_code[0].upper())
            # Return specific letters based on CFI category
            letter_mapping = {
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
            return letter_mapping.get(category, ["C"])
        except ValueError:
            return ["C"]

    @classmethod
    def get_fitrs_patterns_from_firds_file(cls, firds_file_letter: str) -> List[str]:
        """
        Get FITRS file letter patterns from FIRDS file letter.

        Args:
            firds_file_letter: Single letter from FIRDS filename

        Returns:
            List of specific FITRS file letters to search for (e.g., ['E'])
        """
        # Direct mapping - FIRDS letter corresponds to FITRS letter
        return [firds_file_letter.upper()]

    @classmethod
    def get_firds_patterns_from_cfi(cls, cfi_code: str) -> List[str]:
        """
        Get FIRDS file letter patterns from CFI code.

        Args:
            cfi_code: 6-character CFI code

        Returns:
            List of specific FIRDS file letters to search for (e.g., ['E'], ['D'])
        """
        if not cfi_code or len(cfi_code) < 1:
            return ["C", "D", "E", "F", "H", "I", "J", "O", "R", "S"]  # Fallback to all letters

        try:
            category = Category(cfi_code[0].upper())
            # Return specific letters based on CFI category - same as FITRS
            letter_mapping = {
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
            return letter_mapping.get(category, ["C"])
        except ValueError:
            return ["C"]

    @classmethod
    def filter_firds_files_by_cfi(cls, all_files: List[str], cfi_code: str) -> List[str]:
        """
        Filter FIRDS files by CFI code using precise filename matching.

        Args:
            all_files: List of all FIRDS filenames
            cfi_code: 6-character CFI code

        Returns:
            List of matching filenames
        """
        import re

        # Get the target letters for this CFI
        target_letters = cls.get_firds_patterns_from_cfi(cfi_code)
        matching_files = []

        for filename in all_files:
            # Parse filename format: FULINS_{letter}_{date}_{part}_firds_data.csv
            match = re.match(r"^FULINS_([A-Z])_\d{8}_\d+of\d+_firds_data\.csv$", filename)
            if match:
                file_letter = match.group(1)  # C, D, E, F, H, I, J, O, R, S

                # Check if this file letter matches our target
                if file_letter in target_letters:
                    matching_files.append(filename)

        return matching_files

    @classmethod
    def create_cfi_from_firds_context(
        cls, firds_file_letter: str, group: str = "S", attributes: str = "XXXX"
    ) -> str:
        """
        Create a CFI code from FIRDS file context.

        Args:
            firds_file_letter: Single letter from FIRDS filename
            group: CFI group (2nd character), defaults to 'S' for most common types
            attributes: CFI attributes (3rd-6th characters), defaults to 'XXXX'

        Returns:
            6-character CFI code
        """
        category = cls.determine_cfi_from_firds_file(firds_file_letter)
        if category:
            return f"{category.value}{group}{attributes}"
        return f"M{group}{attributes}"  # Default to Others category

    @classmethod
    def validate_cfi_consistency(cls, cfi_code: str, firds_file_letter: str) -> Tuple[bool, str]:
        """
        Validate that CFI code is consistent with FIRDS file letter.

        Args:
            cfi_code: 6-character CFI code
            firds_file_letter: Single letter from FIRDS filename

        Returns:
            Tuple of (is_consistent, error_message)
        """
        if not cfi_code or len(cfi_code) < 1:
            return False, "Invalid CFI code"

        expected_category = cls.determine_cfi_from_firds_file(firds_file_letter)
        if not expected_category:
            return False, f"Invalid FIRDS file letter: {firds_file_letter}"

        actual_category = cfi_code[0].upper()
        expected_letter = expected_category.value

        if actual_category != expected_letter:
            return (
                False,
                f"CFI category '{actual_category}' doesn't match FIRDS file '{firds_file_letter}' (expected '{expected_letter}')",
            )

        return True, ""

    @classmethod
    def get_cfi_info(cls, cfi_code: str) -> Dict:
        """
        Get comprehensive information about a CFI code.

        Args:
            cfi_code: 6-character CFI code

        Returns:
            Dictionary with CFI information including detailed decoding
        """
        try:
            cfi = CFI(cfi_code)
            business_type = cls.get_business_type_from_cfi(cfi_code)
            fitrs_patterns = cls.get_fitrs_patterns_from_cfi(cfi_code)

            # Get the full CFI description including decoded attributes
            cfi_description = cfi.describe()

            return {
                "cfi_code": cfi_code,
                "category": cfi.category,
                "category_description": cfi.category_description,
                "group": cfi.group,
                "group_description": cfi.group_description,
                "attributes": cfi_description.get("attributes", ""),
                "decoded_attributes": cfi_description.get("decoded_attributes", {}),
                "business_type": business_type,
                "fitrs_patterns": fitrs_patterns,
                "is_equity": cfi.is_equity(),
                "is_debt": cfi.is_debt(),
                "is_collective_investment": cfi.is_collective_investment(),
                "is_derivative": cfi.is_derivative(),
            }
        except Exception as e:
            return {
                "error": str(e),
                "cfi_code": cfi_code,
                "business_type": "other",
                "fitrs_patterns": ["FULNCR_C"],
            }

    @classmethod
    def filter_fitrs_files_by_cfi(cls, all_files: List[str], cfi_code: str) -> List[str]:
        """
        Filter FITRS files by CFI code using precise filename matching.

        Args:
            all_files: List of all FITRS filenames
            cfi_code: 6-character CFI code

        Returns:
            List of matching filenames
        """
        import re

        # Get the target letters for this CFI
        target_letters = cls.get_fitrs_patterns_from_cfi(cfi_code)
        matching_files = []

        for filename in all_files:
            # Parse filename format: FUL{ECR|NCR}_{date}_{letter}_{part}_fitrs_data.csv
            match = re.match(r"^FUL(ECR|NCR)_\d{8}_([A-Z])_\d+of\d+_fitrs_data\.csv$", filename)
            if match:
                file_type = match.group(1)  # ECR or NCR
                file_letter = match.group(2)  # C, D, E, F, H, I, J, O, R, S

                # Check if this file letter matches our target
                if file_letter in target_letters:
                    # Additional logic for equity vs non-equity files
                    cfi_category = Category(cfi_code[0].upper())

                    # Equities can appear in both ECR and NCR files
                    # Rights (R) typically appear in ECR files
                    # Most others appear in NCR files
                    if cfi_category == Category.EQUITIES:
                        matching_files.append(filename)  # Accept both ECR and NCR
                    elif cfi_category == Category.ENTITLEMENTS and file_type == "ECR":
                        matching_files.append(filename)  # Rights in ECR files
                    elif cfi_category == Category.COLLECTIVE_INVESTMENT:
                        matching_files.append(filename)  # CIVs can be in both
                    elif file_type == "NCR":
                        matching_files.append(filename)  # Most others in NCR

        return matching_files

    @classmethod
    def validate_instrument_type(cls, instrument_type: str) -> bool:
        """
        Validate if an instrument type is supported by our CFI system.

        Args:
            instrument_type: The instrument type string to validate

        Returns:
            True if valid, False otherwise
        """
        return instrument_type.lower() in cls.CFI_TO_BUSINESS_TYPE.values()

    @classmethod
    def validate_cfi_code(cls, cfi_code: str) -> Tuple[bool, str]:
        """
        Validate if a CFI code is properly formatted and supported.

        Args:
            cfi_code: 6-character CFI code to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not cfi_code:
            return False, "CFI code cannot be empty"

        if len(cfi_code) != 6:
            return False, f"CFI code must be 6 characters, got {len(cfi_code)}"

        try:
            # Try to create CFI object to validate format
            cfi = CFI(cfi_code)
            category = cfi_code[0].upper()

            # Check if we support this category
            if category not in [cat.value for cat in Category]:
                return False, f"Unsupported CFI category: {category}"

            return True, ""
        except Exception as e:
            return False, f"Invalid CFI code format: {str(e)}"

    @classmethod
    def get_valid_instrument_types(cls) -> List[str]:
        """
        Get list of all valid instrument types supported by the CFI system.

        Returns:
            List of valid instrument type strings
        """
        return list(set(cls.CFI_TO_BUSINESS_TYPE.values()))

    @classmethod
    def normalize_instrument_type_from_cfi(cls, cfi_code: str) -> str:
        """
        Get the normalized instrument type from a CFI code.
        This ensures consistency between CFI codes and instrument types.

        Args:
            cfi_code: 6-character CFI code

        Returns:
            Normalized instrument type string
        """
        return cls.get_business_type_from_cfi(cfi_code)


# Convenience functions for easy import
def get_instrument_type_from_cfi(cfi_code: str) -> str:
    """Get instrument type from CFI code - single source of truth"""
    return CFIInstrumentTypeManager.get_business_type_from_cfi(cfi_code)


def get_instrument_type_from_firds_file(firds_file_letter: str) -> str:
    """Get instrument type from FIRDS file letter - CFI-based"""
    return CFIInstrumentTypeManager.get_business_type_from_firds_file(firds_file_letter)


def get_fitrs_patterns_for_cfi(cfi_code: str) -> List[str]:
    """Get FITRS patterns for CFI code - optimized file search"""
    return CFIInstrumentTypeManager.get_fitrs_patterns_from_cfi(cfi_code)


def get_firds_patterns_for_cfi(cfi_code: str) -> List[str]:
    """Get FIRDS patterns for CFI code - optimized file search"""
    return CFIInstrumentTypeManager.get_firds_patterns_from_cfi(cfi_code)


def filter_firds_files_by_cfi(all_files: List[str], cfi_code: str) -> List[str]:
    """Filter FIRDS files by CFI code - precise filename matching"""
    return CFIInstrumentTypeManager.filter_firds_files_by_cfi(all_files, cfi_code)


def filter_fitrs_files_by_cfi(all_files: List[str], cfi_code: str) -> List[str]:
    """Filter FITRS files by CFI code - precise filename matching"""
    return CFIInstrumentTypeManager.filter_fitrs_files_by_cfi(all_files, cfi_code)


def validate_instrument_type(instrument_type: str) -> bool:
    """Validate if an instrument type is supported by the CFI system"""
    return CFIInstrumentTypeManager.validate_instrument_type(instrument_type)


def validate_cfi_code(cfi_code: str) -> tuple:
    """Validate if a CFI code is properly formatted and supported"""
    return CFIInstrumentTypeManager.validate_cfi_code(cfi_code)


def get_valid_instrument_types() -> List[str]:
    """Get list of all valid instrument types supported by the CFI system"""
    return CFIInstrumentTypeManager.get_valid_instrument_types()


def normalize_instrument_type_from_cfi(cfi_code: str) -> str:
    """Get the normalized instrument type from a CFI code"""
    return CFIInstrumentTypeManager.normalize_instrument_type_from_cfi(cfi_code)


def get_firds_letter_for_type(instrument_type: str) -> Optional[str]:
    """
    Get the FIRDS file letter for a given instrument type.
    Returns the letter used in FIRDS filenames (e.g., 'E' for equity).
    """
    try:
        # Create reverse mapping from business type to CFI category
        BUSINESS_TYPE_TO_CFI = {v: k for k, v in CFIInstrumentTypeManager.CFI_TO_BUSINESS_TYPE.items()}
        
        # Get CFI category for instrument type
        cfi_category = BUSINESS_TYPE_TO_CFI.get(instrument_type.lower())
        if not cfi_category:
            return None
        
        # Find the FIRDS letter for this category
        for letter, category in CFIInstrumentTypeManager.FIRDS_TO_CFI_MAPPING.items():
            if category == cfi_category:
                return letter
        
        return None
    except Exception:
        return None
