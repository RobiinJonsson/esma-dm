"""
ISO standard validators for financial identifiers.

This module provides validation functions for:
- ISO 6166: ISIN (International Securities Identification Number)
- ISO 17442: LEI (Legal Entity Identifier)
- ISO 10962: CFI (Classification of Financial Instruments)

All validators follow strict format checking per their respective ISO standards.
"""
from typing import Tuple


def validate_isin(isin: str) -> bool:
    """
    Validate ISIN format according to ISO 6166.
    
    ISIN structure (12 characters):
    - Positions 1-2: Country code (2 letters)
    - Positions 3-11: National Security Identifier (9 alphanumeric)
    - Position 12: Check digit (1 numeric)
    
    Args:
        isin: ISIN code to validate
    
    Returns:
        True if valid ISIN format, False otherwise
    
    Example:
        >>> validate_isin('US0378331005')  # Apple Inc
        True
        >>> validate_isin('GB00B1YW4409')  # Sage Group
        True
        >>> validate_isin('INVALID')
        False
        >>> validate_isin('US037833100')  # Too short
        False
    
    Reference:
        ISO 6166:2021 - Securities and related financial instruments
    """
    if not isinstance(isin, str) or len(isin) != 12:
        return False
    
    # First 2 chars: country code (letters only)
    if not isin[:2].isalpha() or not isin[:2].isupper():
        return False
    
    # Next 9 chars: national security identifier (alphanumeric)
    if not isin[2:11].isalnum():
        return False
    
    # Last char: check digit (numeric)
    if not isin[11].isdigit():
        return False
    
    return True


def validate_lei(lei: str) -> bool:
    """
    Validate LEI format according to ISO 17442.
    
    LEI structure (20 characters):
    - Positions 1-4: LOU identifier (alphanumeric)
    - Positions 5-18: Entity identifier (alphanumeric)
    - Positions 19-20: Check digits (2 numeric)
    
    Args:
        lei: LEI code to validate
    
    Returns:
        True if valid LEI format, False otherwise
    
    Example:
        >>> validate_lei('549300VALTPVHYSYMH70')
        True
        >>> validate_lei('INVALID')
        False
        >>> validate_lei('549300VALTPVHYSYMH7')  # Too short
        False
    
    Reference:
        ISO 17442-1:2020 - Legal entity identifier (LEI)
    """
    if not isinstance(lei, str) or len(lei) != 20:
        return False
    
    # All characters must be alphanumeric and uppercase
    if not lei.isalnum() or not lei.isupper():
        return False
    
    # Characters 1-2 and 19-20 must be numeric for check digits
    # (Note: simplified check - full validation would compute check digits)
    if not lei[18:20].isdigit():
        return False
    
    return True


def validate_cfi(cfi: str) -> bool:
    """
    Validate CFI code format according to ISO 10962.
    
    CFI structure (6 characters):
    - Position 1: Category (letter)
    - Position 2: Group (letter)
    - Positions 3-6: Attributes (letters)
    
    Valid category codes (position 1):
    E=Equities, D=Debt, R=Entitlements, O=Options, F=Futures,
    S=Swaps, H=Non-listed, I=Referential, J=Other, M=Others, C=Currency
    
    Args:
        cfi: CFI code to validate
    
    Returns:
        True if valid CFI format, False otherwise
    
    Example:
        >>> validate_cfi('ESVUFR')  # Common stock
        True
        >>> validate_cfi('DBFUFR')  # Bond
        True
        >>> validate_cfi('INVALID')  # Too long
        False
        >>> validate_cfi('123456')  # Not letters
        False
    
    Reference:
        ISO 10962:2021 - Classification of Financial Instruments (CFI)
    """
    if not isinstance(cfi, str) or len(cfi) != 6:
        return False
    
    # All characters must be letters and uppercase
    if not cfi.isalpha() or not cfi.isupper():
        return False
    
    # First character must be a valid category
    valid_categories = {'E', 'D', 'R', 'O', 'F', 'S', 'H', 'I', 'J', 'M', 'C'}
    if cfi[0] not in valid_categories:
        return False
    
    return True


def validate_mic(mic: str) -> bool:
    """
    Validate MIC (Market Identifier Code) format according to ISO 10383.
    
    MIC structure (4 characters):
    - All uppercase letters or digits
    
    Args:
        mic: MIC code to validate
    
    Returns:
        True if valid MIC format, False otherwise
    
    Example:
        >>> validate_mic('XLON')  # London Stock Exchange
        True
        >>> validate_mic('XNAS')  # NASDAQ
        True
        >>> validate_mic('XNA')  # Too short
        False
    
    Reference:
        ISO 10383:2012 - Market identifier codes (MIC)
    """
    if not isinstance(mic, str) or len(mic) != 4:
        return False
    
    # All characters must be alphanumeric and uppercase (digits don't affect case check)
    if not mic.isalnum() or mic != mic.upper():
        return False
    
    return True


def validate_instrument_identifier(identifier: str, identifier_type: str) -> Tuple[bool, str]:
    """
    Validate instrument identifier based on its type.
    
    Supports multiple identifier types with appropriate validation.
    
    Args:
        identifier: The identifier string to validate
        identifier_type: Type of identifier ('ISIN', 'LEI', 'CFI', 'MIC')
    
    Returns:
        Tuple of (is_valid, error_message)
        - is_valid: True if identifier is valid
        - error_message: Empty string if valid, error description if invalid
    
    Example:
        >>> validate_instrument_identifier('US0378331005', 'ISIN')
        (True, '')
        >>> validate_instrument_identifier('INVALID', 'ISIN')
        (False, 'Invalid ISIN format: must be 12 characters')
    """
    identifier_type = identifier_type.upper()
    
    validators = {
        'ISIN': (validate_isin, 'Invalid ISIN format: must be 12 characters with 2-letter country code'),
        'LEI': (validate_lei, 'Invalid LEI format: must be 20 alphanumeric characters'),
        'CFI': (validate_cfi, 'Invalid CFI format: must be 6 uppercase letters with valid category'),
        'MIC': (validate_mic, 'Invalid MIC format: must be 4 alphanumeric characters')
    }
    
    if identifier_type not in validators:
        return False, f"Unknown identifier type: {identifier_type}"
    
    validator_func, error_msg = validators[identifier_type]
    
    if validator_func(identifier):
        return True, ''
    else:
        return False, error_msg
