"""
Utility enums for FITRS transparency data attributes.

Provides descriptions for abbreviated codes used in ESMA transparency calculations.
"""
from enum import Enum


class Methodology(Enum):
    """Calculation methodologies for transparency assessments (ESMA65-8-5240)."""
    
    SINT = "Systematic Internaliser historical data (discontinued April 2024)"
    YEAR = "Yearly methodology (12-month rolling period)"
    ESTM = "Estimation methodology (insufficient data for yearly)"
    FFWK = "Framework methodology (pre-trade transparency for illiquid instruments)"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get description for methodology code."""
        try:
            return cls[code].value
        except KeyError:
            return f"Unknown methodology: {code}"


class InstrumentClassification(Enum):
    """Financial instrument classifications (ISO 10962 CFI based)."""
    
    SHRS = "Shares (common/ordinary shares)"
    DPRS = "Depositary Receipts (ADRs, GDRs)"
    ETFS = "Exchange Traded Funds"
    OTHR = "Other equity instruments (tracking certificates, etc.)"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get description for instrument classification."""
        try:
            return cls[code].value
        except KeyError:
            return f"Unknown classification: {code}"


class FileType(Enum):
    """FITRS transparency file types."""
    
    FULECR = "Full equity ISIN-level transparency results"
    FULNCR = "Full non-equity ISIN-level transparency results"
    DLTECR = "Delta equity ISIN-level transparency updates"
    DLTNCR = "Delta non-equity ISIN-level transparency updates"
    FULNCR_NYAR = "Full non-equity sub-class yearly results"
    FULNCR_SISC = "Full non-equity sub-class SI results"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get description for file type."""
        try:
            return cls[code].value
        except KeyError:
            return f"Unknown file type: {code}"
    
    @classmethod
    def is_equity(cls, file_type: str) -> bool:
        """Check if file type is equity (ECR)."""
        return 'ECR' in file_type
    
    @classmethod
    def is_non_equity(cls, file_type: str) -> bool:
        """Check if file type is non-equity (NCR)."""
        return 'NCR' in file_type
    
    @classmethod
    def is_subclass(cls, file_type: str) -> bool:
        """Check if file type is sub-class level."""
        return file_type in ['FULNCR_NYAR', 'FULNCR_SISC']
    
    @classmethod
    def is_delta(cls, file_type: str) -> bool:
        """Check if file type is delta (incremental)."""
        return file_type.startswith('DLT')


class SegmentationCriteria(Enum):
    """
    Sub-class segmentation criteria codes (ESMA65-8-5240 Section 2.3).
    
    Used for non-equity sub-class level transparency calculations.
    Each criterion defines how instruments are grouped for liquidity assessment.
    """
    
    # Bond criteria
    BSPD = "Bond: Sovereign, Public, Development (issuer type)"
    SBPD = "Bond: Securitised, Pfandbrief (asset-backed types)"
    FSPD = "Bond: Financial, Supranational, Public Dev Bank (issuer type)"
    CVTB = "Bond: Convertible (embedded equity option)"
    OTHR_BOND = "Bond: Other bonds not in above categories"
    
    # Bond maturity buckets
    TTMB = "Bond: Time To Maturity Bucket (0-1y, 1-2y, 2-3y, etc.)"
    
    # Bond issuance size
    IASZ = "Bond: Issuance Size (small, medium, large)"
    
    # Equity criteria
    MKTC = "Equity: Market Capitalisation bands"
    FRFL = "Equity: Free Float percentage bands"
    
    # Derivatives - Interest Rate
    IRBT = "Interest Rate: Bond Type underlying"
    IRCU = "Interest Rate: Currency"
    IRTT = "Interest Rate: Tenor buckets (time to maturity)"
    IRTI = "Interest Rate: Termination buckets (remaining life)"
    IRNT = "Interest Rate: Notional bands"
    IROT = "Interest Rate: Option Type (call/put)"
    IRST = "Interest Rate: Strike Price bands"
    
    # Derivatives - Equity
    EQTY = "Equity: Underlying equity type"
    EQCU = "Equity: Currency"
    EQPT = "Equity: Parameter Type (price, index, etc.)"
    EQNT = "Equity: Notional bands"
    EQOT = "Equity: Option Type (call/put)"
    EQST = "Equity: Strike Price bands"
    EQTT = "Equity: Time To Expiry buckets"
    
    # Derivatives - Commodity
    CMTY = "Commodity: Commodity type (energy, metals, agricultural)"
    CMCU = "Commodity: Currency"
    CMNT = "Commodity: Notional bands"
    CMTT = "Commodity: Time To Expiry buckets"
    CMUT = "Commodity: Unit Type (barrels, tonnes, etc.)"
    CMOT = "Commodity: Option Type (call/put)"
    CMST = "Commodity: Strike Price bands"
    
    # Derivatives - Foreign Exchange
    FXCU = "FX: Currency Pair"
    FXNT = "FX: Notional bands"
    FXTT = "FX: Time To Expiry buckets"
    FXPT = "FX: Parameter Type (spot, forward, etc.)"
    FXOT = "FX: Option Type (call/put)"
    FXST = "FX: Strike Price bands"
    
    # Derivatives - Credit
    CDBT = "Credit: Bond Type underlying"
    CDCU = "Credit: Currency"
    CDNT = "Credit: Notional bands"
    CDTT = "Credit: Tenor buckets"
    CDTI = "Credit: Termination buckets"
    
    # Other
    UINS = "Underlying ISIN (for derivatives referencing specific instrument)"
    BASK = "Basket of underlyings"
    INDX = "Index underlying"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get description for segmentation criterion code."""
        try:
            return cls[code].value
        except KeyError:
            # Try with OTHR_ prefix for bonds
            try:
                return cls[f"OTHR_{code}"].value
            except KeyError:
                return f"Unknown criterion: {code}"
    
    @classmethod
    def get_category(cls, code: str) -> str:
        """Get asset category for criterion code."""
        if code.startswith(('BSPD', 'SBPD', 'FSPD', 'CVTB', 'OTHR', 'TTMB', 'IASZ')):
            return "Bond"
        elif code.startswith(('MKTC', 'FRFL')):
            return "Equity"
        elif code.startswith('IR'):
            return "Interest Rate Derivative"
        elif code.startswith('EQ'):
            return "Equity Derivative"
        elif code.startswith('CM'):
            return "Commodity Derivative"
        elif code.startswith('FX'):
            return "FX Derivative"
        elif code.startswith('CD'):
            return "Credit Derivative"
        elif code in ['UINS', 'BASK', 'INDX']:
            return "Underlying Reference"
        return "Unknown"


class CalculationType(Enum):
    """Calculation types for sub-class transparency."""
    
    ISIN = "ISIN-level calculation"
    SUBC = "Sub-class level calculation"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get description for calculation type."""
        try:
            return cls[code].value
        except KeyError:
            return f"Unknown calculation type: {code}"


class InstrumentType(Enum):
    """High-level instrument type classification."""
    
    EQUITY = "Equity instruments (shares, ETFs, depositary receipts)"
    NON_EQUITY = "Non-equity instruments (bonds, derivatives)"
    
    @classmethod
    def get_description(cls, code: str) -> str:
        """Get description for instrument type."""
        code_upper = code.upper().replace(' ', '_')
        try:
            return cls[code_upper].value
        except KeyError:
            return f"Unknown instrument type: {code}"


# Helper functions for formatting
def format_methodology_info(code: str) -> dict:
    """
    Get detailed information about a methodology code.
    
    Args:
        code: Methodology code (SINT, YEAR, ESTM, FFWK)
        
    Returns:
        Dictionary with code, description, and additional context
        
    Example:
        >>> info = format_methodology_info('YEAR')
        >>> print(info['description'])
        'Yearly methodology (12-month rolling period)'
    """
    return {
        'code': code,
        'description': Methodology.get_description(code),
        'name': code,
        'valid': code in [m.name for m in Methodology]
    }


def format_classification_info(code: str) -> dict:
    """
    Get detailed information about an instrument classification code.
    
    Args:
        code: Classification code (SHRS, DPRS, ETFS, OTHR)
        
    Returns:
        Dictionary with code, description, and additional context
        
    Example:
        >>> info = format_classification_info('SHRS')
        >>> print(info['description'])
        'Shares (common/ordinary shares)'
    """
    return {
        'code': code,
        'description': InstrumentClassification.get_description(code),
        'name': code,
        'valid': code in [c.name for c in InstrumentClassification]
    }


def format_segmentation_info(code: str) -> dict:
    """
    Get detailed information about a segmentation criterion code.
    
    Args:
        code: Segmentation criterion code (BSPD, TTMB, IRCU, etc.)
        
    Returns:
        Dictionary with code, description, category, and validity
        
    Example:
        >>> info = format_segmentation_info('BSPD')
        >>> print(info['category'])
        'Bond'
        >>> print(info['description'])
        'Bond: Sovereign, Public, Development (issuer type)'
    """
    return {
        'code': code,
        'description': SegmentationCriteria.get_description(code),
        'category': SegmentationCriteria.get_category(code),
        'valid': code in [s.name for s in SegmentationCriteria]
    }
