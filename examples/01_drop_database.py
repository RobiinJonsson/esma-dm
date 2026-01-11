"""
Example: Drop Database

Demonstrates how to safely drop the database to:
1. Start over with a clean slate
2. Ensure no data is stored in memory/disk
3. Free up disk space

Use this when you need to completely remove all data and reinitialize.
"""

from esma_dm import FIRDSClient
from pathlib import Path


def main():
    print("ESMA Data Manager - Drop Database Example")
    print("=" * 60)
    
    # Create FIRDS client
    print("\n1. Creating FIRDS client...")
    firds = FIRDSClient()
    db_path = Path(firds.data_store.db_path)
    
    print(f"   Database path: {db_path}")
    
    # Check if database exists
    if db_path.exists():
        file_size = db_path.stat().st_size
        print(f"   Database exists: YES")
        print(f"   File size: {file_size:,} bytes ({file_size / (1024**2):.2f} MB)")
    else:
        print(f"   Database exists: NO")
    
    # Attempt to drop without confirmation (should fail)
    print("\n2. Testing safety check - drop without confirmation...")
    try:
        firds.data_store.drop()
        print("   ERROR: Drop should have failed without confirmation!")
    except ValueError as e:
        print(f"   Safety check PASSED: {str(e)[:80]}...")
    
    # Drop with confirmation
    print("\n3. Dropping database with confirmation...")
    result = firds.data_store.drop(confirm=True)
    
    print(f"\n   Status: {result['status']}")
    print(f"   Database path: {result['database_path']}")
    print(f"   File existed: {result['existed']}")
    
    if result['existed']:
        print(f"   Deleted size: {result['file_size_bytes']:,} bytes")
    
    # Verify database is gone
    print("\n4. Verifying database deletion...")
    if db_path.exists():
        print("   ERROR: Database file still exists!")
    else:
        print("   SUCCESS: Database file deleted")
    
    # Reinitialize to create fresh database
    print("\n5. Reinitializing fresh database...")
    init_result = firds.data_store.initialize(mode='current')
    
    print(f"\n   Status: {init_result['status']}")
    print(f"   Tables created: {init_result['tables_created']}")
    print(f"   Tables verified: {init_result['tables_verified']}")
    
    # Verify new database exists
    if db_path.exists():
        new_size = db_path.stat().st_size
        print(f"   New database size: {new_size:,} bytes")
    
    print("\n" + "=" * 60)
    print("Database drop and reinitialize complete!")
    print("\nNext steps:")
    print("1. Download data: firds.get_latest_full_files(asset_type='E')")
    print("2. Load database: firds.index_cached_files()")
    print("3. Query data: edm.reference('ISIN')")


if __name__ == "__main__":
    main()
