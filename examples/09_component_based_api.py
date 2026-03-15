"""
Example: Component-Based API Usage

Demonstrates the new FIRDSClient component composition pattern.
The client now exposes clear components with specific responsibilities.
"""
import logging
from esma_dm.clients.firds import FIRDSClient

def main():
    logging.basicConfig(level=logging.INFO)
    
    # Initialize client (current mode)
    firds = FIRDSClient(mode='current')
    
    print("=== Component-Based API Usage ===")
    print()
    
    # 1. Access components directly
    print("1. Component Access:")
    print(f"   Download operations: {type(firds.download).__name__}")
    print(f"   Parse operations:    {type(firds.parse).__name__}")
    print(f"   Storage operations:  {type(firds.store).__name__}")
    print()
    
    # 2. High-level orchestration
    print("2. High-level operations (new):")
    print("   firds.initialize_database()        # Initialize DB schema")
    print("   firds.build_reference_database()   # Download + index workflow")
    print("   firds.get_reference_data('ISIN')   # Quick lookup")
    print("   firds.query_database('SELECT ...')  # Custom SQL")
    print("   firds.get_database_stats()         # Statistics")
    print()
    
    # 3. Component-specific operations
    print("3. Component-specific operations:")
    print("   Download: firds.download.get_latest_full_files(asset_type='E')")
    print("   Download: firds.download.get_file_list()")
    print("   Download: firds.download.get_instruments(['US0378331005'])")
    print()
    print("   Parse: firds.parse.index_cached_files()")
    print("   Parse: firds.parse.reference('US0378331005')")
    print()
    print("   Store: firds.store.get_stats()")
    print("   Store: firds.store.initialize()")
    print()
    
    # 4. History mode features
    print("4. History mode features:")
    print("   firds = FIRDSClient(mode='history')")
    print("   firds.delta.process_delta_files()  # Delta processing")
    print("   firds.process_deltas()             # High-level wrapper")
    print()
    
    # 5. Test database initialization
    print("5. Testing database initialization...")
    try:
        firds.initialize_database()
        stats = firds.get_database_stats()
        print(f"   Database initialized. Table count: {stats.get('table_count', 'N/A')}")
    except Exception as e:
        print(f"   Database error: {e}")
    
    print("\n=== API Improvement Summary ===")
    print("- Reduced from 20+ methods to 6 high-level orchestration methods")
    print("- Clear component boundaries: .download, .parse, .store, .delta")
    print("- Self-documenting API through component grouping")
    print("- Backward compatibility maintained for critical methods")

if __name__ == "__main__":
    main()