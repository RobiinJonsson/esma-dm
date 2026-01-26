"""Full initialization and analysis of all FIRDS asset types."""
from esma_dm.clients.firds import FIRDSClient
import sys

# Set UTF-8 encoding for console output
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("=" * 60)
print("FIRDS Full Initialization and Analysis")
print("=" * 60)

# Initialize client
print("\n[1/3] Initializing FIRDSClient...")
firds = FIRDSClient(mode='current')

# Drop and recreate database
print("\n[2/3] Reinitializing database...")
firds.data_store.drop(confirm=True)
firds.data_store.initialize()

# Index all cached files from latest date
print("\n[3/3] Indexing all cached files from latest date...")
print("Processing ALL asset types (C, D, E, F, H, I, J, O, R, S)...\n")

try:
    result = firds.index_cached_files(latest_only=True)
    print(f"\nIndexing complete!")
    print(f"  Files processed: {result.get('files_processed', 0)}")
    print(f"  Files skipped: {result.get('files_skipped', 0)}")
except Exception as e:
    print(f"\nIndexing failed: {e}")
    sys.exit(1)

# Get database statistics
print("\n" + "=" * 60)
print("Database Statistics")
print("=" * 60)

try:
    stats = firds.data_store.get_stats()
    
    print("\nComprehensive Statistics:")
    print(f"\nTotal instruments: {stats.get('total_instruments', 0):,}")
    print(f"Total listings: {stats.get('total_listings', 0):,}")
    
    print("\nBy Asset Type:")
    asset_stats = stats.get('by_asset_type', {})
    for asset_type, count in sorted(asset_stats.items()):
        print(f"  {asset_type}: {count:,}")
    
    print("\nBy Trading Venue (Top 10):")
    venue_stats = stats.get('by_trading_venue', {})
    for venue, count in sorted(venue_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {venue}: {count:,}")
    
    print("\nStatistics retrieved successfully!")
    
except Exception as e:
    print(f"\nStatistics failed: {e}")
    sys.exit(1)

print("\nComplete!")
