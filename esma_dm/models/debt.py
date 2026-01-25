"""
Debt instrument model for bonds and other debt securities.
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional
from .base import Instrument


@dataclass
class DebtInstrument(Instrument):
    """
    Debt instrument (bonds, notes, etc.) based on actual FIRDS data structure.
    
    Covers CFI codes starting with 'D' (Debt instruments).
    """
    
    # Actual debt-specific attributes from FIRDS
    total_issued_nominal_amount: Optional[float] = None
    """Total issued nominal amount (RefData_DebtInstrmAttrbts_TtlIssdNmnlAmt)"""
    
    maturity_date: Optional[date] = None
    """Maturity date (RefData_DebtInstrmAttrbts_MtrtyDt)"""
    
    nominal_value_per_unit: Optional[float] = None
    """Nominal value per unit (RefData_DebtInstrmAttrbts_NmnlValPerUnit)"""
    
    fixed_interest_rate: Optional[float] = None
    """Fixed interest rate (RefData_DebtInstrmAttrbts_IntrstRate_Fxd)"""
    
    debt_seniority: Optional[str] = None
    """Debt seniority level (RefData_DebtInstrmAttrbts_DebtSnrty)"""
    
    # Interest rate information
    interest_rate_type: Optional[str] = None
    """Type of interest rate: 'fixed', 'floating', or None"""
    
    fixed_rate: Optional[float] = None
    """Fixed interest rate (if applicable)"""
    
    floating_rate_reference_isin: Optional[str] = None
    """Reference rate ISIN for floating rate (if applicable)"""
    
    floating_rate_reference_index: Optional[str] = None
    """Reference index for floating rate (e.g., EURIBOR, LIBOR)"""
    
    floating_rate_reference_name: Optional[str] = None
    """Reference rate name for floating rate"""
    
    floating_rate_term_unit: Optional[str] = None
    """Term unit for floating rate (e.g., MNTH, YEAR)"""
    
    floating_rate_term_value: Optional[int] = None
    """Term value for floating rate"""
    
    floating_rate_basis_points: Optional[float] = None
    """Basis points spread for floating rate"""
    
    @property
    def is_fixed_rate(self) -> bool:
        """Check if this is a fixed rate instrument."""
        return self.fixed_rate is not None
    
    @property
    def is_floating_rate(self) -> bool:
        """Check if this is a floating rate instrument."""
        return (self.floating_rate_reference_isin is not None or 
                self.floating_rate_reference_index is not None)
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for debt-specific fields."""
        base_schema = super().get_schema()
        debt_schema = {
            'total_issued_nominal_amount': {'type': 'float', 'description': 'Total issued nominal amount'},
            'maturity_date': {'type': 'date', 'description': 'Maturity date of the debt instrument'},
            'nominal_value_per_unit': {'type': 'float', 'description': 'Nominal value per unit'},
            'debt_seniority': {'type': 'str', 'description': 'Debt seniority level (SNDB=Senior, SBOD=Subordinated, etc.)'},
            'interest_rate_type': {'type': 'str', 'description': "Type of interest rate: 'fixed', 'floating', or None"},
            'fixed_rate': {'type': 'float', 'description': 'Fixed interest rate (if applicable)'},
            'floating_rate_reference_isin': {'type': 'str', 'description': 'Reference rate ISIN for floating rate (if applicable)'},
            'floating_rate_reference_index': {'type': 'str', 'description': 'Reference index for floating rate (e.g., EURIBOR, LIBOR)'},
            'floating_rate_reference_name': {'type': 'str', 'description': 'Reference rate name for floating rate'},
            'floating_rate_term_unit': {'type': 'str', 'description': 'Term unit for floating rate (e.g., MNTH, YEAR)'},
            'floating_rate_term_value': {'type': 'int', 'description': 'Term value for floating rate'},
            'floating_rate_basis_points': {'type': 'float', 'description': 'Basis points spread for floating rate'},
        }
        return {**base_schema, **debt_schema}
