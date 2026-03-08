"""
Main FIRDS client class that composes all modular components.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
import pandas as pd

from .enums import FileType, AssetType
from .models import FIRDSFile
from .downloader import FIRDSDownloader
from .parser import FIRDSParser
from .delta_processor import FIRDSDeltaProcessor
from esma_dm.config import default_config, get_firds_config
from esma_dm.storage import StorageBackend, DuckDBStorage


class FIRDSClient:
    """
    Client for accessing ESMA FIRDS (Financial Instruments Reference Data System).
    
    FIRDS provides comprehensive reference data for financial instruments including:
    - ISINs and instrument identifiers
    - Instrument classifications (CFI codes)
    - Trading venue information
    - Corporate actions and lifecycle events
    
    Example:
        >>> from esma_dm import FIRDSClient
        >>> 
        >>> # Initialize client with defaults
        >>> firds = FIRDSClient()
        >>> 
        >>> # Initialize client with custom settings
        >>> firds = FIRDSClient(date_from='2025-01-01', limit=500)
        >>> 
        >>> # Download and index latest equity data
        >>> firds.get_latest_full_files(asset_type='E')
        >>> firds.index_cached_files(asset_type='E')
        >>> 
        >>> # Query specific instrument
        >>> apple = firds.reference('US0378331005')
        >>> print(f"Name: {apple['full_name']}")
    """
    
    def __init__(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        limit: Optional[int] = None,
        config=None,
        db_path: Optional[str] = None,
        mode: str = 'current'
    ):
        """
        Initialize FIRDS client.
        
        Args:
            date_from: Start date for file queries (defaults to config default)
            date_to: End date for file queries (defaults to today)
            limit: Maximum number of results per query (defaults to config default)
            config: Configuration object (uses default if None)
            db_path: Custom database path (optional)
            mode: Database mode ('current' or 'history')
        """
        self.config = config or default_config
        self.mode = mode
        
        # Get mode-specific FIRDS configuration
        self.firds_config = get_firds_config(mode)
        
        # Use centralized defaults
        self.date_from, self.date_to = self.firds_config.get_date_range(date_from, date_to)
        self.limit = self.firds_config.validate_limit(limit or self.firds_config.default_limit)
        self.db_path = db_path
        
        self.logger = logging.getLogger(__name__)
        
        # Component composition - expose components directly
        self._data_store = None
        self._downloader = None
        self._parser = None
        self._delta_processor = None
    
    @property
    def data_store(self) -> DuckDBStorage:
        """Lazy-load DuckDB data store."""
        if self._data_store is None:
            cache_dir = self.config.downloads_path / 'firds'
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Get proper database path from config
            db_path = self.db_path or str(self.config.get_database_path(self.mode))
            
            self._data_store = DuckDBStorage(cache_dir, db_path=db_path, mode=self.mode)
        
        return self._data_store
    
    @property
    def downloader(self) -> FIRDSDownloader:
        """Lazy-load downloader component."""
        if self._downloader is None:
            self._downloader = FIRDSDownloader(
                config=self.config,
                firds_config=self.firds_config,
                date_from=self.date_from,
                date_to=self.date_to,
                limit=self.limit
            )
        return self._downloader
    
    # Component Access Properties
    @property
    def download(self) -> FIRDSDownloader:
        """Access to download operations."""
        return self.downloader
    
    @property
    def store(self) -> DuckDBStorage:
        """Access to database storage operations."""
        return self.data_store
    
    @property
    def parse(self) -> FIRDSParser:
        """Access to parsing operations."""
        return self.parser
    
    @property
    def delta(self) -> FIRDSDeltaProcessor:
        """Access to delta processing operations (history mode only)."""
        if self.mode != 'history':
            raise ValueError("Delta processing is only available in history mode")
        return self.delta_processor
    
    @property
    def parser(self) -> FIRDSParser:
        """Lazy-load parser with data store."""
        if self._parser is None:
            self._parser = FIRDSParser(self.config, self.data_store)
        return self._parser
    
    @property
    def delta_processor(self) -> FIRDSDeltaProcessor:
        """Lazy-load delta processor with data store and downloader."""
        if self._delta_processor is None:
            self._delta_processor = FIRDSDeltaProcessor(
                self.config, self.data_store, self.downloader
            )
        return self._delta_processor
    
    # High-level orchestration methods
    def initialize_database(self, mode: Optional[str] = None) -> None:
        """Initialize the database with proper schema for the given mode."""
        target_mode = mode or self.mode
        self.store.initialize(mode=target_mode)
    
    def build_reference_database(
        self,
        asset_types: List[str] = None,
        update: bool = False,
        auto_cleanup: bool = True
    ) -> Dict:
        """Complete workflow: download latest files and index them into database.
        
        This is the main entry point for building a reference database.
        
        Args:
            asset_types: List of asset types to process (defaults to ['E'])
            update: Force re-download of files (defaults to False, uses cache)
            auto_cleanup: Automatically remove old cached files (defaults to True)
            
        Returns:
            Dictionary with processing statistics
        """
        asset_types = asset_types or ['E']
        
        # Only download if explicitly requested
        if update:
            # Download files for all requested asset types
            for asset_type in asset_types:
                self.logger.info(f"Downloading latest files for asset type {asset_type}")
                self.download.get_latest_full_files(
                    asset_type=asset_type, 
                    update=update,
                    auto_cleanup=auto_cleanup
                )
        else:
            self.logger.info("Using existing cached files. Set update=True to download fresh files.")
        
        # Index files (will use available cached files)
        return self.parse.index_cached_files()
    
    def get_reference_data(self, isin: str) -> Optional[pd.Series]:
        """Quick reference data lookup for a single ISIN."""
        return self.parse.reference(isin)
    
    def query_database(self, sql: str) -> pd.DataFrame:
        """Execute custom SQL query against the database."""
        return self.store.con.execute(sql).fetchdf()
    
    def get_database_stats(self) -> Dict:
        """Get comprehensive database statistics (enhanced version)."""
        dashboard = self.get_analytics_dashboard()
        
        if 'error' in dashboard:
            return {'error': dashboard['error']}
        
        # Return a simplified version for compatibility
        return {
            'total_instruments': dashboard['executive_summary']['total_instruments'],
            'total_listings': dashboard['executive_summary']['total_listings'],
            'unique_venues': dashboard['executive_summary']['unique_venues'],
            'asset_breakdown': dashboard['asset_distribution'],
            'mode': dashboard['executive_summary']['database_mode'],
            'last_updated': dashboard['executive_summary']['last_updated']
        }
    
    def get_asset_breakdown(self) -> pd.DataFrame:
        """Get asset type breakdown with percentages."""
        # Get raw counts from data store
        stats_dict = self.store.get_stats_by_asset_type()
        
        if not stats_dict:
            return pd.DataFrame(columns=['asset_type', 'asset_name', 'count', 'percentage'])
        
        # Asset type mappings
        asset_names = {
            'E': 'Equities',
            'D': 'Debt Instruments', 
            'C': 'Collective Investment Vehicles',
            'R': 'Entitlements (Rights)',
            'O': 'Options',
            'F': 'Futures',
            'S': 'Swaps',
            'H': 'Non-Listed Complex Derivatives',
            'I': 'Spot Commodities',
            'J': 'Forwards'
        }
        
        # Calculate total for percentages
        total_count = sum(stats_dict.values())
        
        # Create DataFrame
        data = []
        for asset_type, count in stats_dict.items():
            data.append({
                'asset_type': asset_type,
                'asset_name': asset_names.get(asset_type, f'Unknown ({asset_type})'),
                'count': count,
                'percentage': (count / total_count * 100) if total_count > 0 else 0
            })
        
        return pd.DataFrame(data).sort_values('count', ascending=False)
    
    def get_analytics_dashboard(self) -> Dict[str, Any]:
        """
        Get comprehensive analytics dashboard for the database.
        
        Provides deep insights into data quality, coverage, and distribution
        patterns across all instrument types and trading venues.
        
        Returns:
            Dictionary with comprehensive analytics
        """
        try:
            dashboard = {}
            
            # 1. Executive Summary
            total_instruments = self.store.con.execute(
                "SELECT COUNT(*) as count FROM instruments"
            ).fetchone()[0]
            
            total_listings = self.store.con.execute(
                "SELECT COUNT(*) as count FROM listings"
            ).fetchone()[0]
            
            unique_venues = self.store.con.execute(
                "SELECT COUNT(DISTINCT trading_venue_id) as count FROM listings WHERE trading_venue_id IS NOT NULL"
            ).fetchone()[0]
            
            dashboard['executive_summary'] = {
                'total_instruments': total_instruments,
                'total_listings': total_listings,
                'unique_venues': unique_venues,
                'average_listings_per_instrument': round(total_listings / total_instruments, 2) if total_instruments > 0 else 0,
                'database_mode': self.mode,
                'last_updated': self.store.con.execute(
                    "SELECT MAX(indexed_at) FROM instruments"
                ).fetchone()[0] if total_instruments > 0 else None
            }
            
            # 2. Asset Type Distribution
            asset_breakdown = self.store.con.execute("""
                SELECT 
                    instrument_type as asset_type,
                    COUNT(*) as instrument_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
                    COUNT(DISTINCT issuer) as unique_issuers
                FROM instruments 
                WHERE instrument_type IS NOT NULL
                GROUP BY instrument_type 
                ORDER BY instrument_count DESC
            """).fetchdf()
            
            # Add asset type names
            asset_names = {
                'C': 'Collective Investment Vehicles',
                'D': 'Debt Instruments', 
                'E': 'Equities',
                'F': 'Futures',
                'H': 'Rights & Warrants',
                'I': 'Spot Commodities',
                'J': 'Multi-leg Instruments',
                'O': 'Options',
                'R': 'Referential Instruments',
                'S': 'Swaps'
            }
            
            asset_breakdown['asset_name'] = asset_breakdown['asset_type'].map(asset_names)
            dashboard['asset_distribution'] = asset_breakdown.to_dict('records')
            
            # 3. Venue Analytics
            venue_stats = self.store.con.execute("""
                SELECT 
                    trading_venue_id,
                    COUNT(DISTINCT l.isin) as unique_instruments,
                    COUNT(*) as total_listings,
                    COUNT(DISTINCT i.instrument_type) as asset_types_covered
                FROM listings l
                JOIN instruments i ON l.isin = i.isin
                WHERE trading_venue_id IS NOT NULL
                GROUP BY trading_venue_id
                ORDER BY unique_instruments DESC
                LIMIT 20
            """).fetchdf()
            
            dashboard['top_venues'] = venue_stats.to_dict('records')
            
            # 4. Data Quality Metrics
            data_quality = {}
            
            # Missing data analysis
            missing_data = self.store.con.execute("""
                SELECT 
                    'short_name' as field,
                    COUNT(*) - COUNT(short_name) as missing_count,
                    ROUND((COUNT(*) - COUNT(short_name)) * 100.0 / COUNT(*), 2) as missing_percentage
                FROM instruments
                UNION ALL
                SELECT 
                    'issuer' as field,
                    COUNT(*) - COUNT(issuer) as missing_count,
                    ROUND((COUNT(*) - COUNT(issuer)) * 100.0 / COUNT(*), 2) as missing_percentage
                FROM instruments
                UNION ALL
                SELECT 
                    'full_name' as field,
                    COUNT(*) - COUNT(full_name) as missing_count,
                    ROUND((COUNT(*) - COUNT(full_name)) * 100.0 / COUNT(*), 2) as missing_percentage
                FROM instruments
            """).fetchdf()
            
            data_quality['missing_data'] = missing_data.to_dict('records')
            
            # ISIN format validation
            isin_quality = self.store.con.execute("""
                SELECT 
                    CASE 
                        WHEN LENGTH(isin) = 12 THEN 'Valid Length'
                        ELSE 'Invalid Length'
                    END as isin_status,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM instruments
                GROUP BY LENGTH(isin)
            """).fetchdf()
            
            data_quality['isin_quality'] = isin_quality.to_dict('records')
            
            dashboard['data_quality'] = data_quality
            
            # 5. Temporal Analysis
            temporal_stats = {}
            
            # Most recent vs oldest data
            date_range = self.store.con.execute("""
                SELECT 
                    MIN(indexed_at) as earliest_data,
                    MAX(indexed_at) as latest_data,
                    COUNT(DISTINCT DATE(indexed_at)) as unique_index_dates
                FROM instruments
            """).fetchone()
            
            temporal_stats['data_freshness'] = {
                'earliest_data': date_range[0],
                'latest_data': date_range[1],
                'unique_index_dates': date_range[2]
            }
            
            # Source file diversity
            source_files = self.store.con.execute("""
                SELECT 
                    source_file,
                    COUNT(*) as instruments_count,
                    MIN(indexed_at) as processed_at
                FROM instruments
                GROUP BY source_file
                ORDER BY processed_at DESC
                LIMIT 10
            """).fetchdf()
            
            temporal_stats['source_files'] = source_files.to_dict('records')
            
            dashboard['temporal_analysis'] = temporal_stats
            
            # 6. Market Structure Insights
            market_insights = {}
            
            # Instruments with multiple listings (cross-listed)
            cross_listed = self.store.con.execute("""
                SELECT 
                    COUNT(*) as instruments_with_multiple_venues,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT isin) FROM listings), 2) as cross_listing_rate
                FROM (
                    SELECT isin 
                    FROM listings 
                    GROUP BY isin 
                    HAVING COUNT(DISTINCT trading_venue_id) > 1
                ) cross_listed_instruments
            """).fetchone()
            
            market_insights['cross_listing_stats'] = {
                'instruments_with_multiple_venues': cross_listed[0],
                'cross_listing_percentage': cross_listed[1]
            }
            
            # Average listings per asset type
            listings_per_type = self.store.con.execute("""
                SELECT 
                    i.instrument_type as asset_type,
                    ROUND(AVG(listing_count), 2) as avg_listings_per_instrument,
                    MAX(listing_count) as max_listings_single_instrument
                FROM instruments i
                JOIN (
                    SELECT isin, COUNT(*) as listing_count
                    FROM listings
                    GROUP BY isin
                ) l_counts ON i.isin = l_counts.isin
                GROUP BY i.instrument_type
                ORDER BY avg_listings_per_instrument DESC
            """).fetchdf()
            
            market_insights['listings_distribution'] = listings_per_type.to_dict('records')
            
            dashboard['market_insights'] = market_insights
            
            # 7. Storage Efficiency
            storage_stats = self.store.con.execute("""
                SELECT 
                    'instruments' as table_name,
                    COUNT(*) as row_count
                FROM instruments
                UNION ALL
                SELECT 
                    'listings' as table_name,
                    COUNT(*) as row_count
                FROM listings
                UNION ALL
                SELECT 
                    'equity_instruments' as table_name,
                    COUNT(*) as row_count
                FROM equity_instruments
                UNION ALL
                SELECT 
                    'debt_instruments' as table_name,
                    COUNT(*) as row_count
                FROM debt_instruments
                UNION ALL
                SELECT 
                    'swap_instruments' as table_name,
                    COUNT(*) as row_count
                FROM swap_instruments
                UNION ALL
                SELECT 
                    'civ_instruments' as table_name,
                    COUNT(*) as row_count
                FROM civ_instruments
            """).fetchdf()
            
            dashboard['storage_stats'] = storage_stats.to_dict('records')
            
            return dashboard
            
        except Exception as e:
            self.logger.error(f"Failed to generate analytics dashboard: {e}")
            return {'error': str(e)}
    
    def print_analytics_summary(self) -> None:
        """Print a formatted analytics summary to console."""
        dashboard = self.get_analytics_dashboard()
        
        if 'error' in dashboard:
            print(f"❌ Analytics failed: {dashboard['error']}")
            return
        
        print("=" * 80)
        print("🏦 ESMA FIRDS DATABASE ANALYTICS DASHBOARD")
        print("=" * 80)
        
        # Executive Summary
        exec_summary = dashboard['executive_summary']
        print(f"\n📊 EXECUTIVE SUMMARY")
        print(f"   Database Mode: {exec_summary['database_mode'].upper()}")
        print(f"   Total Instruments: {exec_summary['total_instruments']:,}")
        print(f"   Total Listings: {exec_summary['total_listings']:,}")
        print(f"   Unique Trading Venues: {exec_summary['unique_venues']:,}")
        print(f"   Avg Listings per Instrument: {exec_summary['average_listings_per_instrument']}")
        if exec_summary['last_updated']:
            print(f"   Last Updated: {exec_summary['last_updated']}")
        
        # Asset Distribution
        print(f"\n🏷️  ASSET TYPE DISTRIBUTION")
        for asset in dashboard['asset_distribution'][:5]:  # Top 5
            print(f"   {asset['asset_type']} - {asset['asset_name']}: "
                  f"{asset['instrument_count']:,} ({asset['percentage']}%)")
        
        # Top Venues
        print(f"\n🏛️  TOP TRADING VENUES")
        for venue in dashboard['top_venues'][:5]:  # Top 5
            print(f"   {venue['trading_venue_id']}: "
                  f"{venue['unique_instruments']:,} instruments, "
                  f"{venue['total_listings']:,} listings")
        
        # Market Insights
        market = dashboard['market_insights']
        print(f"\n📈 MARKET STRUCTURE INSIGHTS")
        print(f"   Cross-listed Instruments: {market['cross_listing_stats']['instruments_with_multiple_venues']:,} "
              f"({market['cross_listing_stats']['cross_listing_percentage']}%)")
        
        # Data Quality
        print(f"\n✅ DATA QUALITY METRICS")
        for quality_check in dashboard['data_quality']['missing_data']:
            if quality_check['missing_percentage'] < 10:
                status = "✅"
            elif quality_check['missing_percentage'] < 25:
                status = "⚠️"
            else:
                status = "❌"
            print(f"   {status} {quality_check['field']}: "
                  f"{quality_check['missing_percentage']}% missing")
        
        print(f"\n💾 STORAGE DISTRIBUTION")
        for storage in dashboard['storage_stats']:
            print(f"   {storage['table_name']}: {storage['row_count']:,} rows")
        
        print("=" * 80)
    
    # History mode operations
    def process_deltas(
        self,
        asset_type: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        update: bool = False
    ) -> Dict:
        """Process delta files with version management (history mode only).
        
        This is the main entry point for incremental updates in history mode.
        """
        if self.mode != 'history':
            raise ValueError(f"Delta file processing is only available in history mode, not {self.mode} mode.")
        
        return self.delta.process_delta_files(
            asset_type=asset_type, date_from=date_from, date_to=date_to, update=update
        )
    
    # Backward compatibility (keep minimal delegation for critical methods)
    def index_cached_files(
        self,
        asset_type: Optional[str] = None,
        latest_only: bool = True,
        file_type: str = 'FULINS',
        delete_csv: bool = False
    ) -> Dict:
        """Index downloaded CSV files into the database with mode validation."""
        # Mode-based file type validation
        if self.mode == 'current' and file_type != 'FULINS':
            raise ValueError(f"Current mode only supports FULINS files, not {file_type}. Use history mode for DLTINS processing.")
        
        # Check if appropriate files exist (use existing files by default)
        cache_dir = self.config.downloads_path / 'firds'
        if asset_type:
            pattern = f"{file_type}_{asset_type}_*_data.csv"
        else:
            pattern = f"{file_type}_*_data.csv"
        
        existing_files = list(cache_dir.glob(pattern))
        
        # Only download if explicitly no files exist and user hasn't indicated they want to use cache
        if not existing_files and file_type == 'FULINS':
            self.logger.info(f"No {file_type} files found for pattern {pattern}.")
            self.logger.info("To download fresh files, use: firds.download.get_latest_full_files(update=True)")
            self.logger.info("To use existing files, call: firds.parse.index_cached_files() directly")
            
            # Check if there are any FULINS files at all
            any_fulins = list(cache_dir.glob("FULINS_*_data.csv"))
            if any_fulins:
                self.logger.info(f"Found {len(any_fulins)} existing FULINS files. Consider:")
                self.logger.info("  - Remove asset_type filter: firds.index_cached_files(asset_type=None)")
                self.logger.info("  - Or specify different asset_type: firds.index_cached_files(asset_type='D')")
                return {
                    'total_instruments': 0,
                    'total_listings': 0,
                    'files_processed': 0,
                    'files_skipped': 0,
                    'failed_files': [f"No files matching pattern {pattern}. Use existing files instead."],
                    'asset_types_processed': []
                }
        
        return self.parse.index_cached_files(
            asset_type=asset_type, latest_only=latest_only, 
            file_type=file_type, delete_csv=delete_csv
        )