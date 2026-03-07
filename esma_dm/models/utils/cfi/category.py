"""ISO 10962 CFI top-level category codes."""

from enum import Enum


class Category(Enum):
    """ISO 10962 Financial Instrument Categories (position 1 of CFI code)."""

    EQUITIES = "E"                  # Equities
    DEBT = "D"                      # Debt instruments
    COLLECTIVE_INVESTMENT = "C"     # Collective investment vehicles (CIVs)
    ENTITLEMENTS = "R"              # Entitlements (rights)
    OPTIONS = "O"                   # Listed options
    FUTURES = "F"                   # Futures
    SWAPS = "S"                     # Swaps
    NON_STANDARD = "H"              # Non-listed and complex listed options
    SPOT = "I"                      # Spot
    FORWARDS = "J"                  # Forwards
    STRATEGIES = "K"                # Strategies
    FINANCING = "L"                 # Financing
    REFERENTIAL = "T"               # Referential instruments
    OTHERS = "M"                    # Others/miscellaneous
