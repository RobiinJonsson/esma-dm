"""
Base instrument model with common attributes across all asset types.
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, List
from enum import Enum


class RecordType(Enum):
    """Type of record in DLTINS files."""
    MODIFIED = "ModfdRcrd"
    NEW = "NewRcrd"
    TERMINATED = "TermntdRcrd"


@dataclass
class TradingVenueAttributes:
    """Trading venue related attributes."""
    
    venue_id: Optional[str] = None
    """Trading venue MIC code"""
    
    issuer_request: Optional[bool] = None
    """Whether admission was requested by issuer"""
    
    admission_approval_date: Optional[date] = None
    """Date of admission approval by issuer"""
    
    request_for_admission_date: Optional[date] = None
    """Date of request for admission"""
    
    first_trade_date: Optional[date] = None
    """Date of first trade"""
    
    termination_date: Optional[date] = None
    """Date of termination"""
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for this dataclass."""
        return {
            'venue_id': {'type': 'str', 'description': 'Trading venue identifier'},
            'issuer_request': {'type': 'str', 'description': 'Issuer request indicator'},
            'admission_approval_date': {'type': 'date', 'description': 'Date of admission approval by issuer'},
            'request_for_admission_date': {'type': 'date', 'description': 'Date of request for admission'},
            'first_trade_date': {'type': 'date', 'description': 'First trade date'},
            'termination_date': {'type': 'date', 'description': 'Date of termination'},
        }


@dataclass
class TechnicalAttributes:
    """Technical/administrative attributes."""
    
    relevant_competent_authority: Optional[str] = None
    """Relevant competent authority (country code)"""
    
    publication_period_from: Optional[date] = None
    """Publication period start date"""
    
    relevant_trading_venue: Optional[str] = None
    """Relevant trading venue"""
    
    never_published: Optional[bool] = None
    """Flag indicating if record was never published"""
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for this dataclass."""
        return {
            'relevant_competent_authority': {'type': 'str', 'description': 'Relevant competent authority (country code)'},
            'publication_period_from': {'type': 'date', 'description': 'Publication period start date'},
            'relevant_trading_venue': {'type': 'str', 'description': 'Relevant trading venue'},
            'never_published': {'type': 'bool', 'description': 'Flag indicating if record was never published'},
        }


@dataclass
class Instrument:
    """
    Base instrument class with common attributes for all financial instruments.
    
    This represents the core reference data that exists across all asset types
    including equities, bonds, derivatives, etc.
    """
    
    # Core identifiers
    isin: str
    """International Securities Identification Number"""
    
    full_name: str
    """Full name of the instrument"""
    
    short_name: Optional[str] = None
    """Short name of the instrument"""
    
    classification_type: Optional[str] = None
    """CFI code - Classification of Financial Instruments"""
    
    notional_currency: Optional[str] = None
    """Notional currency (ISO 4217 code)"""
    
    # Issuer information
    issuer: Optional[str] = None
    """Issuer LEI (Legal Entity Identifier)"""
    
    # Commodity derivative indicator
    commodity_derivative_indicator: bool = False
    """Whether this is a commodity derivative"""
    
    # Related attributes
    trading_venue: TradingVenueAttributes = field(default_factory=TradingVenueAttributes)
    """Trading venue related attributes"""
    
    technical: TechnicalAttributes = field(default_factory=TechnicalAttributes)
    """Technical/administrative attributes"""
    
    # DLTINS specific
    record_type: Optional[RecordType] = None
    """Type of record (Modified/New/Terminated) - only in DLTINS files"""
    
    reporting_date: Optional[date] = None
    """Reporting period date - only in DLTINS files"""
    
    reporting_authority: Optional[str] = None
    """National competent authority reporting - only in DLTINS files"""
    
    @property
    def asset_type(self) -> Optional[str]:
        """
        Extract asset type from CFI code (first character).
        
        Returns:
            Asset type: C=Collective Investment, D=Debt, E=Equity, F=Futures,
            H=Rights, I=Options, J=Strategies, O=Others, R=Referential, S=Swaps
        """
        if self.classification_type and len(self.classification_type) > 0:
            return self.classification_type[0]
        return None
    
    @property
    def instrument_category(self) -> Optional[str]:
        """Extract instrument category from CFI code (second character)."""
        if self.classification_type and len(self.classification_type) > 1:
            return self.classification_type[1]
        return None    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for base Instrument fields."""
        return {
            'isin': {'type': 'str', 'required': True, 'description': 'International Securities Identification Number'},
            'full_name': {'type': 'str', 'required': True, 'description': 'Full instrument name'},
            'short_name': {'type': 'str', 'description': 'Short instrument name'},
            'classification_type': {'type': 'str', 'description': 'CFI code (ISO 10962)'},
            'notional_currency': {'type': 'str', 'description': 'Notional currency (ISO 4217)'},
            'issuer': {'type': 'str', 'description': 'Issuer LEI code'},
            'commodity_derivative_indicator': {'type': 'str', 'description': 'Commodity derivative indicator (true/false)'},
            'trading_venue': {'type': 'TradingVenueAttributes', 'description': 'Trading venue attributes'},
            'technical': {'type': 'TechnicalAttributes', 'description': 'Technical record attributes'},
            'record_type': {'type': 'RecordType', 'description': 'Record type for DLTINS (MODIFIED/NEW/TERMINATED)'},
            'reporting_date': {'type': 'date', 'description': 'Reporting date (DLTINS only)'},
            'reporting_authority': {'type': 'str', 'description': 'Reporting authority (DLTINS only)'},
        }    
    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(isin='{self.isin}', name='{self.short_name or self.full_name}')"
