"""
Equity instrument model for shares and equity securities.
"""
from dataclasses import dataclass
from typing import Optional
from .base import Instrument


@dataclass
class EquityInstrument(Instrument):
    """
    Equity instrument (shares, stocks, etc.) with equity-specific attributes.
    
    Covers CFI codes starting with 'E' (Equities).
    """
    
    # Equity-specific attributes
    dividend_payment_frequency: Optional[str] = None
    """Frequency of dividend payments"""
    
    voting_rights_per_share: Optional[str] = None
    """Voting rights per share (e.g., DVOT=Voting, DNVT=Non-voting, DVTX=Voting/No voting)"""
    
    ownership_restriction: Optional[str] = None
    """Ownership restrictions on the equity"""
    
    redemption_type: Optional[str] = None
    """Redemption type (e.g., REDF=Redeemable/Callable, NRDF=Non-redeemable)"""
    
    capital_investment_restriction: Optional[str] = None
    """Capital investment restrictions"""
    
    @property
    def has_voting_rights(self) -> bool:
        """Check if this equity has voting rights."""
        if self.voting_rights_per_share is None:
            return False
        return self.voting_rights_per_share in ('DVOT', 'DVTX')
    
    @property
    def is_redeemable(self) -> bool:
        """Check if this equity is redeemable."""
        return self.redemption_type == 'REDF'
    
    @classmethod
    def get_schema(cls) -> dict:
        """Get schema information for equity-specific fields."""
        base_schema = super().get_schema()
        equity_schema = {
            'dividend_payment_frequency': {'type': 'str', 'description': 'Frequency of dividend payments'},
            'voting_rights_per_share': {'type': 'str', 'description': 'Voting rights per share (e.g., DVOT=Voting, DNVT=Non-voting, DVTX=Voting/No voting)'},
            'ownership_restriction': {'type': 'str', 'description': 'Ownership restrictions on the equity'},
            'redemption_type': {'type': 'str', 'description': 'Redemption type (e.g., REDF=Redeemable/Callable, NRDF=Non-redeemable)'},
            'capital_investment_restriction': {'type': 'str', 'description': 'Capital investment restrictions'},
        }
        return {**base_schema, **equity_schema}
