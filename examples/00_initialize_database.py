"""
Example: Initialize Database

Demonstrates the correct workflow for setting up esma-dm:
1. Initialize database
2. Verify schema structure
3. Check initialization status

Run this script before downloading or loading any data.
"""

from esma_dm import FIRDSClient


def main():
    print("ESMA Data Manager - Database Initialization Example")
    print("=" * 60)
    
    # Create FIRDS client
    print("\n1. Creating FIRDS client...")
    firds = FIRDSClient()
    print(f"   Database path: {firds.data_store.db_path}")
    
    # Initialize database
    print("\n2. Initializing database...")
    result = firds.data_store.initialize(mode='current')
    
    print(f"\n   Status: {result['status']}")
    print(f"   Mode: {result['mode']}")
    print(f"   Tables created: {result['tables_created']}")
    print(f"   Tables verified: {result['tables_verified']}")
    
    if result['verification']['all_verified']:
        print("   Schema verification: PASSED")
        print(f"   Verified tables: {', '.join(sorted(result['verification']['verified_tables']))}")
    else:
        print("   Schema verification: FAILED")
        print(f"   Errors: {result['verification']['errors']}")
        return
    
    # Verify existing database
    print("\n3. Verifying existing database structure...")
    verify_result = firds.data_store.initialize(verify_only=True)
    
    print(f"\n   Status: {verify_result['status']}")
    print(f"   Tables verified: {verify_result['tables_verified']}")
    print(f"   Existing tables: {len(verify_result['existing_tables'])}")
    
    print("\n" + "=" * 60)
    print("Database initialization complete!")
    print("\nNext steps:")
    print("1. Download data: firds.get_latest_full_files(asset_type='E')")
    print("2. Load database: firds.index_cached_files()")
    print("3. Query data: edm.reference('ISIN')")


if __name__ == "__main__":
    main()
