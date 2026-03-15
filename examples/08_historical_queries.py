"""
Historical Database Queries (ESMA Section 8 Compliance)

Demonstrates temporal query capabilities per ESMA65-8-5014 Section 8 guidance.
The database maintains complete version history with valid_from_date, valid_to_date,
and latest_record_flag for regulatory audit trails.
"""

import esma_dm as edm
from datetime import datetime, timedelta

def main():
    # Initialize FIRDS client
    firds = edm.FIRDSClient()
    
    # Check database exists
    try:
        total = firds.data_store.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        print(f"Database contains {total:,} instruments\n")
        
        if total == 0:
            print("Database is empty. Run these commands first:")
            print("  1. firds.data_store.initialize(mode='current')")
            print("  2. firds.get_latest_full_files(asset_type='E')")
            print("  3. firds.index_cached_files()")
            return
    except Exception as e:
        print(f"Database not initialized: {e}")
        print("Run: firds.data_store.initialize(mode='current')")
        return
    
    # Example 1: Get current/latest versions only
    print("=== Example 1: Latest Versions (Current State) ===")
    latest = firds.data_store.get_latest_instruments(limit=5)
    print(f"Latest instruments: {len(latest)}")
    if not latest.empty:
        print(f"Columns: {list(latest.columns)[:10]}")
        print(f"First ISIN: {latest.iloc[0]['isin']}")
        print(f"Latest flag: {latest.iloc[0]['latest_record_flag']}")
        print()
    
    # Example 2: Historical state on specific date
    print("=== Example 2: Instrument State on Specific Date ===")
    # Try to find an instrument with history
    result = firds.data_store.con.execute("""
        SELECT isin FROM instruments 
        WHERE valid_from_date IS NOT NULL 
        LIMIT 1
    """).fetchone()
    
    if result:
        test_isin = result[0]
        target_date = '2024-06-15'
        state = firds.data_store.get_instrument_state_on_date(test_isin, target_date)
        
        if state:
            print(f"ISIN: {test_isin}")
            print(f"State on {target_date}:")
            print(f"  Valid from: {state.get('valid_from_date')}")
            print(f"  Valid to: {state.get('valid_to_date')}")
            print(f"  Record type: {state.get('record_type')}")
            print(f"  Version: {state.get('version_number')}")
        else:
            print(f"No state found for {test_isin} on {target_date}")
    else:
        print("No instruments with historical data found")
    print()
    
    # Example 3: Instruments active on specific date
    print("=== Example 3: Instruments Active on Date ===")
    # Query instruments that were trading on a specific date
    active_date = '2025-01-15'
    active = firds.data_store.get_instruments_active_on_date(active_date, limit=5)
    print(f"Instruments active on {active_date}: {len(active)}")
    if not active.empty:
        for idx, row in active.head(3).iterrows():
            print(f"  {row['isin']} - {row.get('short_name', 'N/A')}")
    print()
    
    # Example 4: Version history for specific instrument
    print("=== Example 4: Complete Version History ===")
    # Find an instrument with multiple versions
    versions_query = firds.data_store.con.execute("""
        SELECT isin, COUNT(*) as version_count
        FROM instrument_history
        GROUP BY isin
        HAVING COUNT(*) > 1
        ORDER BY version_count DESC
        LIMIT 1
    """).fetchone()
    
    if versions_query:
        isin_with_history = versions_query[0]
        versions = firds.data_store.get_instrument_version_history(isin_with_history)
        
        print(f"ISIN: {isin_with_history}")
        print(f"Total versions: {len(versions)}")
        print("\nVersion history:")
        for _, version in versions.iterrows():
            print(f"  v{version['version_number']}: "
                  f"{version['valid_from_date']} to {version.get('valid_to_date', 'current')} "
                  f"({version['record_type']})")
    else:
        print("No instruments with version history found yet")
        print("(Version history is populated by delta file processing)")
    print()
    
    # Example 5: Modified instruments since date
    print("=== Example 5: Recently Modified Instruments ===")
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    modified = firds.data_store.get_modified_instruments_since(thirty_days_ago)
    print(f"Instruments modified since {thirty_days_ago}: {len(modified)}")
    
    if not modified.empty:
        # Group by record type
        by_type = modified.groupby('record_type').size()
        print("\nModifications by type:")
        for record_type, count in by_type.items():
            print(f"  {record_type}: {count}")
    print()
    
    # Example 6: Cancelled instruments
    print("=== Example 6: Cancelled Instruments (FULCAN) ===")
    cancelled = firds.data_store.get_cancelled_instruments(since_date='2024-01-01')
    print(f"Instruments cancelled since 2024-01-01: {len(cancelled)}")
    
    if not cancelled.empty:
        print("\nRecent cancellations:")
        for _, cancel in cancelled.head(5).iterrows():
            print(f"  {cancel['isin']} - {cancel.get('cancellation_reason', 'No reason')}")
            print(f"    Cancelled: {cancel['cancellation_date']}")
    else:
        print("No cancellations found")
        print("(FULCAN files must be downloaded and processed)")
    print()
    
    # Example 7: Database statistics
    print("=== Example 7: Historical Database Statistics ===")
    stats = firds.data_store.con.execute("""
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN latest_record_flag = TRUE THEN 1 ELSE 0 END) as current_records,
            COUNT(DISTINCT record_type) as record_types,
            MIN(valid_from_date) as earliest_date,
            MAX(valid_from_date) as latest_date
        FROM instruments
        WHERE valid_from_date IS NOT NULL
    """).fetchdf()
    
    if not stats.empty and stats.iloc[0]['total_records'] > 0:
        s = stats.iloc[0]
        print(f"Total records with temporal data: {s['total_records']:,}")
        print(f"Current versions: {s['current_records']:,}")
        print(f"Historical versions: {s['total_records'] - s['current_records']:,}")
        print(f"Record types tracked: {s['record_types']}")
        print(f"Date range: {s['earliest_date']} to {s['latest_date']}")
    else:
        print("No temporal tracking data found yet")
        print("Initialize with: firds.data_store.initialize(mode='history')")
    
    print("\n" + "="*60)
    print("Historical database enables regulatory compliance:")
    print("  - Point-in-time queries (instrument state on any date)")
    print("  - Audit trails (complete version history)")
    print("  - Change tracking (modifications since date)")
    print("  - Cancellation tracking (FULCAN file support)")
    print("="*60)

if __name__ == "__main__":
    main()
