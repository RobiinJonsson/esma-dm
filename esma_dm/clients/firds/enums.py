"""
FIRDS enumerations and constants.
"""

from enum import Enum


class FileType(Enum):
    """FIRDS file types."""
    FULINS = "FULINS"  # Full instrument snapshot
    DLTINS = "DLTINS"  # Delta (incremental updates)


class AssetType(Enum):
    """CFI first character representing asset types (ISO 10962)."""
    COLLECTIVE_INVESTMENT = "C"  # Collective investment vehicles
    DEBT = "D"  # Debt instruments (bonds, notes)
    EQUITY = "E"  # Equities (shares, units)
    FUTURES = "F"  # Futures
    RIGHTS = "H"  # Rights, warrants
    OPTIONS = "I"  # Options
    STRATEGIES = "J"  # Strategies, multi-leg
    OTHERS = "O"  # Others (misc)
    REFERENTIAL = "R"  # Referential instruments
    SWAPS = "S"  # Swaps


class CommodityBaseProduct(Enum):
    """Commodity base product classifications."""
    AGRI = "AGRI"  # Agricultural
    NRGY = "NRGY"  # Energy
    ENVR = "ENVR"  # Environmental
    EMIS = "EMIS"  # Emissions
    FRGT = "FRGT"  # Freight
    FRTL = "FRTL"  # Fertilizer
    INDP = "INDP"  # Industrial Products
    METL = "METL"  # Metals
    POLY = "POLY"  # Polypropylene / Plastics
    INFL = "INFL"  # Inflation
    OEST = "OEST"  # Official Economic Statistics
    OTHR = "OTHR"  # Other


class OptionType(Enum):
    """Option type classifications."""
    CALL = "CALL"  # Call option
    PUT = "PUT"   # Put option
    OTHR = "OTHR"  # Other


class ExerciseStyle(Enum):
    """Option exercise style."""
    EURO = "EURO"  # European (exercise only at expiry)
    AMER = "AMER"  # American (exercise anytime)
    BRMN = "BRMN"  # Bermudan (exercise at specific dates)
    ASIA = "ASIA"  # Asian (average price)


class DeliveryType(Enum):
    """Settlement delivery type."""
    PHYS = "PHYS"  # Physical delivery
    CASH = "CASH"  # Cash settlement
    OPTL = "OPTL"  # Optional (choice)


class BondSeniority(Enum):
    """Bond seniority classifications."""
    SNDB = "SNDB"  # Senior debt
    MZZD = "MZZD"  # Mezzanine
    SBOD = "SBOD"  # Subordinated
    JUND = "JUND"  # Junior