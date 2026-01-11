"""
Example: Index Cached Files with Filters

Demonstrates the enhanced index_cached_files() method with various options:
1. Filter by asset type
2. Select latest files only
3. Process all file versions
4. Delete CSV after indexing

This shows how to selectively load data into the database.
"""

from esma_dm import FIRDSClient


def main():
    print("ESMA Data Manager - Index Cached Files Example")
    print("=" * 60)
    
    # Create FIRDS client
    print("\n1. Creating FIRDS client...")
    firds = FIRDSClient()
    
    # Drop and reinitialize for clean test
    print("\n2. Initializing fresh database...")
    firds.data_store.drop(confirm=True)
    firds.data_store.initialize(mode='current')
    
    # Example 1: Index only equities (latest only)
    print("\n3. Indexing latest equity files only...")
    result = firds.index_cached_files(asset_type='E', latest_only=True)
    
    print(f"\n   Files processed: {result['files_processed']}")
    print(f"   Files skipped: {result['files_skipped']}")
    print(f"   Instruments: {result['total_instruments']:,}")
    print(f"   Listings: {result['total_listings']:,}")
    print(f"   Asset types: {result['asset_types_processed']}")
    
    # Check database stats
    stats = firds.get_store_stats()
    print(f"\n   Database total: {stats['total_instruments']:,} instruments")
    
    # Example 2: Add debt instruments
    print("\n4. Adding latest debt instrument files...")
    result = firds.index_cached_files(asset_type='D', latest_only=True)
    
    print(f"\n   Files processed: {result['files_processed']}")
    print(f"   Instruments added: {result['total_instruments']:,}")
    print(f"   Listings added: {result['total_listings']:,}")
    
    # Check updated stats
    stats = firds.get_store_stats()
    print(f"\n   Database total: {stats['total_instruments']:,} instruments")
    
    # Example 3: Show all cached files (would process if latest_only=False)
    print("\n5. Checking what would happen with latest_only=False...")
    cache_dir = firds.config.downloads_path / 'firds'
    all_files = sorted(cache_dir.glob("FULINS_E_*.csv"))
    print(f"\n   Total equity files in cache: {len(all_files)}")
    if all_files:
        print(f"   Oldest: {all_files[0].name}")
        print(f"   Newest: {all_files[-1].name}")
    
    # Example 4: Index all asset types (latest only)
    print("\n6. Demonstration: Index all asset types at once...")
    print("   (Skipped to avoid processing, but would work like this:)")
    print("   result = firds.index_cached_files(latest_only=True)")
    print("   This processes latest file for each asset type: C,D,E,F,H,I,J,O,R,S")
    
    print("\n" + "=" * 60)
    print("Index cached files examples complete!")
    print("\nKey parameters:")
    print("- asset_type: Filter to specific type (E, D, etc.) or None for all")
    print("- latest_only: True (default) uses newest file, False processes all")
    print("- file_type: 'FULINS' (default) for snapshots, 'DLTINS' for deltas")
    print("- delete_csv: False (default) keeps files, True deletes after indexing")


if __name__ == "__main__":
    main()
