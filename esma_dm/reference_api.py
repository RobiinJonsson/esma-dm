"""
Reference API for convenient access to FIRDS reference data.

Provides hierarchical access to instrument data:
- edm.reference('ISIN') - Get reference data for an ISIN
- edm.reference.swap.types() - Get all unique swap CFI codes
- edm.reference.equity.count() - Get count of equity instruments
"""
from typing import List, Dict, Optional, Any
import pandas as pd


class AssetTypeAPI:
    """API for a specific asset type."""
    
    def __init__(self, asset_type: str, client):
        self.asset_type = asset_type
        self.client = client
        
    def types(self) -> pd.DataFrame:
        """
        Get all unique CFI codes for this asset type.
        
        Returns:
            DataFrame with columns: cfi_code, count, description
        """
        storage = self.client.data_store
        
        query = """
            SELECT 
                cfi_code,
                COUNT(*) as count
            FROM instruments
            WHERE instrument_type = ?
            GROUP BY cfi_code
            ORDER BY count DESC
        """
        
        result = storage.con.execute(query, [self.asset_type]).fetchdf()
        
        # Add CFI descriptions
        if len(result) > 0:
            from .models.utils import CFI
            descriptions = []
            for cfi_code in result['cfi_code']:
                try:
                    cfi = CFI(cfi_code)
                    descriptions.append(cfi.describe())
                except:
                    descriptions.append('Unknown')
            result['description'] = descriptions
        
        return result[['cfi_code', 'count', 'description']] if len(result) > 0 else result
    
    def count(self) -> int:
        """Get total count of instruments for this asset type."""
        storage = self.client.data_store
        result = storage.con.execute(
            "SELECT COUNT(*) FROM instruments WHERE instrument_type = ?",
            [self.asset_type]
        ).fetchone()
        return result[0] if result else 0
    
    def sample(self, limit: int = 10) -> pd.DataFrame:
        """
        Get sample instruments of this asset type.
        
        Args:
            limit: Number of samples to return
            
        Returns:
            DataFrame with instrument details
        """
        storage = self.client.data_store
        return storage.con.execute(
            """
            SELECT isin, cfi_code, full_name, currency
            FROM instruments
            WHERE instrument_type = ?
            LIMIT ?
            """,
            [self.asset_type, limit]
        ).fetchdf()


class ReferenceAPI:
    """
    Main reference API providing both callable interface and asset type access.
    
    Usage:
        >>> import esma_dm as edm
        >>> # Direct reference lookup
        >>> instrument = edm.reference('SE0000242455')
        >>> 
        >>> # Asset type queries
        >>> swap_types = edm.reference.swap.types()
        >>> equity_count = edm.reference.equity.count()
    """
    
    def __init__(self, client=None):
        from .clients.firds import FIRDSClient
        self._client = client or FIRDSClient()
        
        # Create asset type APIs
        self._equity = AssetTypeAPI('E', self._client)
        self._debt = AssetTypeAPI('D', self._client)
        self._civ = AssetTypeAPI('C', self._client)
        self._futures = AssetTypeAPI('F', self._client)
        self._options = AssetTypeAPI('O', self._client)
        self._swap = AssetTypeAPI('S', self._client)
        self._referential = AssetTypeAPI('H', self._client)
        self._rights = AssetTypeAPI('R', self._client)
        self._spot = AssetTypeAPI('I', self._client)
        self._forward = AssetTypeAPI('J', self._client)
    
    def __call__(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Get reference data for an ISIN.
        
        Args:
            isin: ISIN code to look up
            
        Returns:
            Dictionary containing instrument reference data
        """
        return self._client.get_reference_data(isin)
    
    @property
    def equity(self) -> AssetTypeAPI:
        """Access equity instruments (E)."""
        return self._equity
    
    @property
    def debt(self) -> AssetTypeAPI:
        """Access debt instruments (D)."""
        return self._debt
    
    @property
    def civ(self) -> AssetTypeAPI:
        """Access collective investment vehicles (C)."""
        return self._civ
    
    @property
    def futures(self) -> AssetTypeAPI:
        """Access futures instruments (F)."""
        return self._futures
    
    @property
    def options(self) -> AssetTypeAPI:
        """Access options instruments (O)."""
        return self._options
    
    @property
    def swap(self) -> AssetTypeAPI:
        """Access swap instruments (S)."""
        return self._swap
    
    @property
    def referential(self) -> AssetTypeAPI:
        """Access referential instruments (H)."""
        return self._referential
    
    @property
    def rights(self) -> AssetTypeAPI:
        """Access rights/entitlements (R)."""
        return self._rights
    
    @property
    def spot(self) -> AssetTypeAPI:
        """Access spot instruments (I)."""
        return self._spot
    
    @property
    def forward(self) -> AssetTypeAPI:
        """Access forward instruments (J)."""
        return self._forward
    
    def types(self) -> pd.DataFrame:
        """
        Get all unique CFI codes across all asset types.
        
        Returns:
            DataFrame with columns: instrument_type, cfi_code, count, description
        """
        storage = self._client.data_store
        
        query = """
            SELECT 
                instrument_type,
                cfi_code,
                COUNT(*) as count
            FROM instruments
            GROUP BY instrument_type, cfi_code
            ORDER BY instrument_type, count DESC
        """
        
        result = storage.con.execute(query).fetchdf()
        
        # Add CFI descriptions
        if len(result) > 0:
            from .models.utils import CFI
            descriptions = []
            for cfi_code in result['cfi_code']:
                try:
                    cfi = CFI(cfi_code)
                    descriptions.append(cfi.describe())
                except:
                    descriptions.append('Unknown')
            result['description'] = descriptions
        
        return result
    
    def summary(self) -> pd.DataFrame:
        """
        Get summary statistics for all asset types.
        
        Returns:
            DataFrame with count per asset type
        """
        storage = self._client.data_store
        
        return storage.con.execute("""
            SELECT 
                instrument_type,
                COUNT(*) as count,
                COUNT(DISTINCT cfi_code) as unique_cfi_codes
            FROM instruments
            GROUP BY instrument_type
            ORDER BY instrument_type
        """).fetchdf()
    
    def subtypes(self) -> pd.DataFrame:
        """
        Get all available subtype output models with their CFI prefixes.
        
        Returns:
            DataFrame with columns: cfi_prefix, model_name, description, volume_estimate
            
        Example:
            >>> import esma_dm as edm
            >>> subtypes = edm.reference.subtypes()
            >>> print(subtypes)
            
            cfi_prefix  model_name         description                    volume_estimate
            SE          EquitySwap         Equity Total Return Swaps      1,138,694
            HR          Swaption           Interest Rate Swaptions        956,107
            HE          EquityOption       OTC Equity Options             559,533
            RF          MiniFuture         Mini-Futures/Leverage Certs    4,466,144
            ...
        """
        from .models.subtypes import SUBTYPE_MODELS
        
        # Define subtype information
        subtype_info = {
            'SE': {
                'description': 'Equity Total Return Swaps',
                'volume': 1138694,
                'fields': 28
            },
            'HR': {
                'description': 'Interest Rate Swaptions (Options on Swaps)',
                'volume': 956107,
                'fields': 38
            },
            'HE': {
                'description': 'OTC Equity Options (Non-Standardized)',
                'volume': 559533,
                'fields': 33
            },
            'RF': {
                'description': 'Mini-Futures / Constant Leverage Certificates',
                'volume': 4466144,
                'fields': 50
            },
            'EY': {
                'description': 'Structured Equity Participation Certificates',
                'volume': 771474,
                'fields': 28
            },
            'DE': {
                'description': 'Structured Debt without Capital Protection',
                'volume': 939717,
                'fields': 29
            },
            'FC': {
                'description': 'Commodity Futures',
                'volume': 28977,
                'fields': 56
            },
            'JF': {
                'description': 'Foreign Exchange Forwards',
                'volume': 212732,
                'fields': 22
            },
        }
        
        # Build DataFrame
        data = []
        for cfi_prefix, model_class in SUBTYPE_MODELS.items():
            info = subtype_info.get(cfi_prefix, {})
            data.append({
                'cfi_prefix': cfi_prefix,
                'model_name': model_class.__name__,
                'description': info.get('description', 'Unknown'),
                'volume_estimate': f"{info.get('volume', 0):,}",
                'firds_fields': info.get('fields', 0)
            })
        
        df = pd.DataFrame(data)
        df = df.sort_values('volume_estimate', ascending=False, 
                           key=lambda x: x.str.replace(',', '').astype(int))
        
        return df
