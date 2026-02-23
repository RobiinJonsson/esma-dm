"""
Shared enumerations for ESMA file management.

Contains enums used across FIRDS, FITRS, and other ESMA data sources.
"""

from enum import Enum


# ============================================================================
# File Types
# ============================================================================

class FIRDSFileType(Enum):
    """FIRDS file types."""
    FULINS = "FULINS"  # Full instrument snapshot
    DLTINS = "DLTINS"  # Delta (incremental updates)
    FULCAN = "FULCAN"  # Full cancellations


class FITRSFileType(Enum):
    """FITRS file types."""
    FULECR = "FULECR"  # Full Equity Comprehensive Report
    DLTECR = "DLTECR"  # Delta Equity Comprehensive Report
    FULNCR = "FULNCR"  # Full Non-Equity Comprehensive Report
    DLTNCR = "DLTNCR"  # Delta Non-Equity Comprehensive Report
    FULNCR_NYAR = "FULNCR_NYAR"  # Non-Equity Subclass Yearly
    FULNCR_SISC = "FULNCR_SISC"  # Non-Equity Subclass SI


class DVCAPFileType(Enum):
    """DVCAP (Double Volume Cap) file types."""
    DVCRES = "DVCRES"  # Volume cap results


class BenchmarksFileType(Enum):
    """Benchmarks file types."""
    BENCH_ARCHIVE = "BENCH_ARCHIVE"  # Benchmark archive files


# ============================================================================
# Asset Types (CFI first character per ISO 10962)
# ============================================================================

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


# ============================================================================
# Instrument Classifications
# ============================================================================

class InstrumentType(Enum):
    """High-level instrument type classification."""
    EQUITY = "Equity Instruments"
    NON_EQUITY = "Non-Equity Instruments"


# ============================================================================
# Commodity Classifications
# ============================================================================

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


# ============================================================================
# Option and Derivative Types
# ============================================================================

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


# ============================================================================
# Bond Classifications
# ============================================================================

class BondSeniority(Enum):
    """Bond seniority classifications."""
    SNDB = "SNDB"  # Senior debt
    MZZD = "MZZD"  # Mezzanine
    SBOD = "SBOD"  # Subordinated
    JUND = "JUND"  # Junior


# ============================================================================
# Compatibility Aliases
# ============================================================================

# For backward compatibility with existing code
FileType = FIRDSFileType  # Default to FIRDS file types
