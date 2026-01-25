"""
ESMA Data Manager - Initialization Script

This script helps users set up their ESMA data environment with proper guidance
on choosing between current and history modes.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from esma_dm import FIRDSClient
from esma_dm.utils.constants import ASSET_TYPE_CODES


def print_banner():
    """Print a welcome banner."""
    print("=" * 70)
    print("🏦 ESMA Data Manager - Initialization")
    print("=" * 70)
    print("Welcome! This script will help you set up your ESMA data environment.")
    print("Please follow the prompts to configure your database.\n")


def explain_modes() -> str:
    """Explain the difference between current and history modes."""
    print("📋 DATABASE MODES EXPLAINED")
    print("-" * 40)
    print("\n🔄 CURRENT MODE (Recommended for most users)")
    print("  • Stores only the latest snapshot of each instrument")
    print("  • Optimized for fast queries and low storage usage")
    print("  • Automatically updates with latest available data")
    print("  • Best for: Trading, analysis, reference lookups")
    print("  • Database size: ~500MB for all instruments")
    
    print("\n📚 HISTORY MODE (Advanced users)")
    print("  • Maintains complete historical versions of all instruments")
    print("  • Tracks all changes with full audit trail")
    print("  • Required for regulatory compliance (ESMA Section 8.2)")
    print("  • Supports delta processing and point-in-time queries")
    print("  • Database size: ~5GB+ (grows with time)")
    print("  • Best for: Compliance, auditing, research")
    
    print("\n" + "=" * 50)
    
    while True:
        choice = input("Choose mode [current/history]: ").lower().strip()
        if choice in ['current', 'history']:
            return choice
        print("❌ Please enter 'current' or 'history'")


def get_asset_types() -> list:
    """Let user choose which asset types to initialize."""
    print("\n📊 ASSET TYPES AVAILABLE")
    print("-" * 30)
    print("Available asset types (based on CFI codes):")
    for code, description in ASSET_TYPE_CODES.items():
        print(f"  {code}: {description}")
    
    print("\nRecommended combinations:")
    print("  • 'E' - Equities only (fastest, ~100MB)")
    print("  • 'E,D' - Equities and Bonds (common setup)")
    print("  • 'S' - Swaps only (derivatives trading)")
    print("  • 'all' - All asset types (complete dataset)")
    
    while True:
        choice = input("\nWhich asset types? [E,D/all/custom]: ").strip()
        
        if choice.lower() == 'all':
            return list(ASSET_TYPE_CODES.keys())
        elif choice.upper() in ['E', 'D', 'S', 'F', 'O']:
            return [choice.upper()]
        elif ',' in choice:
            types = [t.strip().upper() for t in choice.split(',')]
            valid_types = [t for t in types if t in ASSET_TYPE_CODES]
            if valid_types:
                return valid_types
        elif choice == '':
            return ['E', 'D']  # Default
        
        print("❌ Please enter valid asset types (e.g., 'E,D' or 'all')")


def get_history_config() -> Tuple[str, str]:
    """Get configuration for history mode."""
    print("\n📅 HISTORY MODE CONFIGURATION")
    print("-" * 35)
    print("History mode requires a starting date for data collection.")
    print("The system will download full files for this date, then process")
    print("delta files to build a complete historical record.\n")
    
    # Suggest a reasonable start date (6 months ago)
    suggested_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    print(f"Suggested start date: {suggested_date} (6 months ago)")
    print("Note: Earlier dates require more download time and storage space.")
    
    while True:
        start_date = input(f"\nStart date [YYYY-MM-DD] or press Enter for {suggested_date}: ").strip()
        
        if not start_date:
            start_date = suggested_date
        
        try:
            # Validate date format
            datetime.strptime(start_date, '%Y-%m-%d')
            break
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD")
    
    # Ask about delta processing
    print("\n🔄 DELTA PROCESSING")
    print("Process daily delta files to keep history up-to-date?")
    print("  • Yes: Complete historical record (recommended)")
    print("  • No: Only initial snapshot")
    
    while True:
        process_deltas = input("Process delta files? [Y/n]: ").lower().strip()
        if process_deltas in ['', 'y', 'yes']:
            return start_date, 'yes'
        elif process_deltas in ['n', 'no']:
            return start_date, 'no'
        print("❌ Please enter 'y' or 'n'")


def check_storage_space(mode: str, asset_types: list) -> bool:
    """Check if user has sufficient storage space."""
    # Rough estimates
    estimates = {
        'current': {'E': 100, 'D': 200, 'S': 300, 'F': 50, 'O': 150, 'other': 100},
        'history': {'E': 500, 'D': 1000, 'S': 1500, 'F': 200, 'O': 400, 'other': 300}
    }
    
    total_mb = sum(estimates[mode].get(asset_type, estimates[mode]['other']) 
                   for asset_type in asset_types)
    
    print(f"\n💾 STORAGE REQUIREMENTS")
    print("-" * 25)
    print(f"Mode: {mode.upper()}")
    print(f"Asset types: {', '.join(asset_types)}")
    print(f"Estimated storage needed: ~{total_mb}MB")
    
    if total_mb > 1000:
        print(f"⚠️  Large dataset ({total_mb/1000:.1f}GB) - ensure sufficient disk space")
    
    return True


def initialize_database(mode: str, asset_types: list, history_config: Optional[Tuple[str, str]] = None):
    """Initialize the database with chosen configuration."""
    print(f"\n🚀 INITIALIZING DATABASE")
    print("-" * 30)
    
    try:
        # Create client
        print(f"Creating FIRDSClient in {mode} mode...")
        client = FIRDSClient(mode=mode)
        
        # Initialize schema
        print("Initializing database schema...")
        result = client.data_store.initialize()
        print(f"✅ Database initialized: {result['database_path']}")
        
        # Download and index data for each asset type
        for asset_type in asset_types:
            print(f"\n📥 Processing {asset_type} ({ASSET_TYPE_CODES[asset_type]})...")
            
            try:
                # Download latest files
                print(f"  Downloading latest {asset_type} files...")
                client.get_latest_full_files(asset_type=asset_type)
                
                # Index into database
                print(f"  Indexing {asset_type} data...")
                stats = client.index_cached_files(asset_type=asset_type)
                
                if stats['total_instruments'] > 0:
                    print(f"  ✅ {stats['total_instruments']} instruments indexed")
                else:
                    print(f"  ⚠️  No instruments indexed (may be empty dataset)")
                    
            except Exception as e:
                print(f"  ❌ Failed to process {asset_type}: {e}")
                continue
        
        # Process deltas for history mode
        if mode == 'history' and history_config and history_config[1] == 'yes':
            start_date, _ = history_config
            print(f"\n📈 Processing delta files from {start_date}...")
            
            for asset_type in asset_types:
                try:
                    delta_stats = client.process_delta_files(
                        asset_type=asset_type,
                        date_from=start_date
                    )
                    print(f"  ✅ {asset_type}: {delta_stats.get('files_processed', 0)} delta files processed")
                except Exception as e:
                    print(f"  ⚠️  {asset_type} delta processing failed: {e}")
        
        # Final statistics
        print(f"\n📊 FINAL STATISTICS")
        print("-" * 20)
        stats = client.get_store_stats()
        print(f"Database mode: {mode}")
        print(f"Database path: {client.data_store.db_path}")
        print(f"Total instruments: {stats.get('total_instruments', 0)}")
        
        if stats.get('asset_type_counts'):
            print("By asset type:")
            for asset_type, count in stats['asset_type_counts'].items():
                name = ASSET_TYPE_CODES.get(asset_type, asset_type)
                print(f"  {asset_type} ({name}): {count}")
        
        print(f"\n✅ SUCCESS! Your ESMA database is ready.")
        print("\nNext steps:")
        print("  • Import: from esma_dm import FIRDSClient")
        print("  • Query: import esma_dm as edm; edm.reference('YOUR_ISIN')")
        print("  • See examples/ folder for usage patterns")
        
    except Exception as e:
        print(f"\n❌ INITIALIZATION FAILED: {e}")
        print("\nTroubleshooting:")
        print("  • Check internet connection")
        print("  • Verify sufficient disk space")
        print("  • Try again with fewer asset types")
        sys.exit(1)


def main():
    """Main initialization script."""
    print_banner()
    
    # Choose mode
    mode = explain_modes()
    
    # Choose asset types
    asset_types = get_asset_types()
    
    # Get history configuration if needed
    history_config = None
    if mode == 'history':
        history_config = get_history_config()
    
    # Check storage requirements
    check_storage_space(mode, asset_types)
    
    # Final confirmation
    print(f"\n🔍 CONFIGURATION SUMMARY")
    print("-" * 25)
    print(f"Mode: {mode.upper()}")
    print(f"Asset types: {', '.join(asset_types)}")
    if history_config:
        start_date, process_deltas = history_config
        print(f"Start date: {start_date}")
        print(f"Process deltas: {'Yes' if process_deltas == 'yes' else 'No'}")
    
    print(f"\nThis will create a database in the package data directory.")
    
    while True:
        confirm = input("\nProceed with initialization? [Y/n]: ").lower().strip()
        if confirm in ['', 'y', 'yes']:
            break
        elif confirm in ['n', 'no']:
            print("Initialization cancelled.")
            sys.exit(0)
        print("❌ Please enter 'y' or 'n'")
    
    # Initialize
    initialize_database(mode, asset_types, history_config)


if __name__ == "__main__":
    main()