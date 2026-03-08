"""
Transparency API for FITRS data access.
"""
from typing import Optional, Dict, Any
import pandas as pd
from esma_dm.clients.fitrs import FITRSClient


class TransparencyAPI:
    """API for accessing FITRS transparency data."""
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize Transparency API.
        
        Args:
            db_path: Override path to the unified DuckDB file. Defaults to the path
            resolved by Config (esma_dm/storage/duckdb/database/esma_current.duckdb).
        """
        self.client = FITRSClient(db_path=db_path)
    
    def __call__(self, isin: str) -> Optional[Dict[str, Any]]:
        """
        Get transparency data for an ISIN.
        
        Args:
            isin: ISIN code
            
        Returns:
            Dictionary with transparency metrics or None if not found
            
        Example:
            >>> edm = ESMADataManager()
            >>> transparency_data = edm.transparency('GB00B1YW4409')
            >>> print(transparency_data['liquid_market'])
            >>> print(transparency_data['average_daily_turnover'])
        """
        return self.client.transparency(isin)
    
    def index(
        self,
        file_type: str = 'FULECR',
        asset_type: Optional[str] = None,
        latest_only: bool = True
    ) -> Dict[str, Any]:
        """
        Index transparency data into database.
        
        Supports:
        - FULECR: Equity ISIN-level full results
        - FULNCR: Non-equity ISIN-level full results  
        - DLTECR: Equity ISIN-level delta (incremental updates)
        - DLTNCR: Non-equity ISIN-level delta (incremental updates)
        - FULNCR_NYAR: Non-equity sub-class yearly results
        - FULNCR_SISC: Non-equity sub-class SI results
        
        Args:
            file_type: Type of files to index
            asset_type: Optional asset type filter (for ISIN-level files)
            latest_only: Only process latest files per asset type
            
        Returns:
            Dictionary with processing statistics
            
        Example:
            >>> edm = ESMADataManager()
            >>> # Index equity ISIN-level data
            >>> result = edm.transparency.index('FULECR')
            >>> print(f"Indexed {result['total_records']} instruments")
            >>> 
            >>> # Index equity delta updates
            >>> result = edm.transparency.index('DLTECR', latest_only=True)
            >>> 
            >>> # Index non-equity sub-class data
            >>> result = edm.transparency.index('FULNCR_NYAR')
        """
        return self.client.index_transparency_data(
            file_type=file_type,
            asset_type=asset_type,
            latest_only=latest_only
        )
    
    def query(
        self,
        liquid_only: bool = False,
        instrument_classification: Optional[str] = None,
        instrument_type: Optional[str] = None,
        min_turnover: Optional[float] = None,
        most_relevant_market: Optional[str] = None,
        methodology: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query ISIN-level transparency data with filters.
        
        Args:
            liquid_only: Only return liquid instruments
            instrument_classification: Filter by classification (SHRS, DPRS, OTHR, ETFS)
            instrument_type: Filter by type ('equity' or 'non_equity')
            min_turnover: Minimum average daily turnover
            most_relevant_market: Filter by most relevant market ID
            methodology: Filter by methodology (SINT, YEAR, ESTM, FFWK)
            limit: Maximum number of results
            
        Returns:
            DataFrame with transparency data
            
        Example:
            >>> edm = ESMADataManager()
            >>> liquid = edm.transparency.query(liquid_only=True, min_turnover=1e6)
            >>> yearly = edm.transparency.query(methodology='YEAR')
        """
        return self.client.query_transparency(
            liquid_only=liquid_only,
            instrument_classification=instrument_classification,
            instrument_type=instrument_type,
            min_turnover=min_turnover,
            most_relevant_market=most_relevant_market,
            methodology=methodology,
            limit=limit
        )
    
    def query_subclass(
        self,
        asset_class: Optional[str] = None,
        sub_asset_class_code: Optional[str] = None,
        liquid_only: bool = False,
        methodology: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Query sub-class level transparency data with filters.
        
        Args:
            asset_class: Asset class code
            sub_asset_class_code: Sub-asset class code
            liquid_only: Only return liquid sub-classes
            methodology: Filter by methodology (YEAR, ESTM, FFWK)
            limit: Maximum number of results
            
        Returns:
            DataFrame with sub-class transparency data
            
        Example:
            >>> edm = ESMADataManager()
            >>> liquid_bonds = edm.transparency.query_subclass(
            ...     asset_class='BOND',
            ...     liquid_only=True
            ... )
        """
        return self.client.query_subclass_transparency(
            asset_class=asset_class,
            sub_asset_class_code=sub_asset_class_code,
            liquid_only=liquid_only,
            methodology=methodology,
            limit=limit
        )
    
    def attach_firds(self, firds_db_path: Optional[str] = None):
        """
        Attach FIRDS database for cross-database queries.
        
        Args:
            firds_db_path: Deprecated. FIRDS and FITRS tables now share the same
                unified database — no attach required. This parameter is ignored.
            
        Example:
            >>> edm = ESMADataManager()
            >>> edm.transparency.attach_firds()
            >>> # Now can use cross-database queries
            >>> sql = '''
            ... SELECT f.isin, f.full_name, t.liquid_market, t.average_daily_turnover
            ... FROM firds.instruments f
            ... JOIN transparency t ON f.isin = t.isin
            ... WHERE t.liquid_market = 'Y'
            ... '''
            >>> results = edm.transparency.client.data_store.query(sql)
        """
        if firds_db_path is None:
            from pathlib import Path
            firds_db_path = str(Path(self.client.data_store.db_path).parent / "firds.db")
        
        self.client.data_store.attach_firds_database(firds_db_path)
