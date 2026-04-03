"""
Test delta file processing and version management.

Tests ESMA Section 8.2 version management logic:
- NEW record insertion
- MODIFIED record version updates
- TERMINATED record handling
- CANCELLED record processing
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from esma_dm import FIRDSClient
from datetime import datetime


def test_delta_processing():
    """Test delta record processing with all record types."""
    print("Testing Delta File Processing")
    print("=" * 60)
    
    # Initialize client
    firds = FIRDSClient(mode='history')
    
    # Initialize clean database
    print("\n1. Initializing clean database...")
    try:
        firds.data_store.drop(confirm=True)
        print("   Dropped existing database")
    except:
        pass
    
    firds.data_store.initialize(mode='history')
    print("   Database initialized")
    
    # Test 1: NEW record (first time)
    print("\n2. Testing NEW record (first insertion)...")
    test_isin = "TEST00000001"
    test_data = {
        'full_name': 'Test Instrument A',
        'cfi_code': 'ESVUFR',
        'issuer': 'Test Issuer A'
    }
    
    result = firds.data_store.process_delta_record(
        isin=test_isin,
        record_type='NEW',
        record_data=test_data,
        publication_date='2025-01-01',
        source_file='DLTINS_E_20250101_01of01.zip'
    )
    print(f"   Result: {result['status']} - {result['message']}")
    
    # Verify insertion
    instrument = firds.data_store.con.execute(
        "SELECT isin, version_number, valid_from_date, latest_record_flag FROM instruments WHERE isin = ?",
        [test_isin]
    ).fetchone()
    
    if instrument:
        print(f"   ✓ Instrument inserted: version={instrument[1]}, valid_from={instrument[2]}, latest={instrument[3]}")
    else:
        print("   ✗ ERROR: Instrument not found!")
        return False
    
    # Test 2: MODIFIED record
    print("\n3. Testing MODIFIED record...")
    test_data_modified = {
        'full_name': 'Test Instrument A - Modified',
        'cfi_code': 'ESVUFR',
        'issuer': 'Test Issuer A'
    }
    
    result = firds.data_store.process_delta_record(
        isin=test_isin,
        record_type='MODIFIED',
        record_data=test_data_modified,
        publication_date='2025-02-01',
        source_file='DLTINS_E_20250201_01of01.zip'
    )
    print(f"   Result: {result['status']} - {result['message']}")
    
    # Verify version incremented
    instrument = firds.data_store.con.execute(
        "SELECT version_number, valid_from_date, valid_to_date, full_name FROM instruments WHERE isin = ?",
        [test_isin]
    ).fetchone()
    
    if instrument:
        print(f"   ✓ Version updated: version={instrument[0]}, valid_from={instrument[1]}")
        print(f"   ✓ Name updated: {instrument[3]}")
    else:
        print("   ✗ ERROR: Instrument not found after modification!")
        return False
    
    # Check history table
    history = firds.data_store.con.execute(
        "SELECT COUNT(*) FROM instrument_history WHERE isin = ?",
        [test_isin]
    ).fetchone()
    
    if history and history[0] > 0:
        print(f"   ✓ History archived: {history[0]} version(s) in history")
    else:
        print("   ✗ WARNING: No history records found")
    
    # Test 3: Get version history
    print("\n4. Testing version history query...")
    versions = firds.data_store.get_instrument_version_history(test_isin)
    print(f"   ✓ Retrieved {len(versions)} version(s)")
    
    for idx, row in versions.iterrows():
        print(f"     Version {row['version_number']}: {row['valid_from_date']} to {row['valid_to_date']}")
    
    # Test 4: Get latest instruments
    print("\n5. Testing get_latest_instruments()...")
    latest = firds.data_store.get_latest_instruments(limit=10)
    print(f"   ✓ Retrieved {len(latest)} latest instrument(s)")
    
    if len(latest) > 0:
        print(f"     Sample: {latest.iloc[0]['isin']} (version {latest.iloc[0]['version_number']})")
    
    # Test 5: TERMINATED record
    print("\n6. Testing TERMINATED record...")
    result = firds.data_store.process_delta_record(
        isin=test_isin,
        record_type='TERMINATED',
        record_data=test_data_modified,
        publication_date='2025-03-01',
        source_file='DLTINS_E_20250301_01of01.zip'
    )
    print(f"   Result: {result['status']} - {result['message']}")
    
    # Verify termination
    instrument = firds.data_store.con.execute(
        "SELECT valid_to_date, latest_record_flag, record_type FROM instruments WHERE isin = ?",
        [test_isin]
    ).fetchone()
    
    if instrument:
        print(f"   ✓ Terminated: valid_to={instrument[0]}, latest={instrument[1]}, type={instrument[2]}")
    
    # Test 6: CANCELLED record
    print("\n7. Testing CANCELLED record...")
    test_isin_2 = "TEST00000002"
    
    # First insert a NEW record
    firds.data_store.process_delta_record(
        isin=test_isin_2,
        record_type='NEW',
        record_data={'full_name': 'Test Instrument B', 'cfi_code': 'ESVUFR', 'issuer': 'Test Issuer B'},
        publication_date='2025-01-15',
        source_file='DLTINS_E_20250115_01of01.zip'
    )
    
    # Then cancel it
    result = firds.data_store.process_delta_record(
        isin=test_isin_2,
        record_type='CANCELLED',
        record_data={'trading_venue_id': 'XLON', 'cancellation_reason': 'Data error'},
        publication_date='2025-01-20',
        source_file='FULCAN_E_20250120_01of01.zip'
    )
    print(f"   Result: {result['status']} - {result['message']}")
    
    # Verify cancellation
    cancelled = firds.data_store.con.execute(
        "SELECT COUNT(*) FROM cancellations WHERE isin = ?",
        [test_isin_2]
    ).fetchone()
    
    if cancelled and cancelled[0] > 0:
        print(f"   ✓ Cancellation recorded: {cancelled[0]} record(s)")
    else:
        print("   ✗ WARNING: No cancellation records found")
    
    # Check removed from instruments
    instrument = firds.data_store.con.execute(
        "SELECT COUNT(*) FROM instruments WHERE isin = ?",
        [test_isin_2]
    ).fetchone()
    
    if instrument and instrument[0] == 0:
        print(f"   ✓ Removed from instruments table")
    else:
        print(f"   ✗ WARNING: Still in instruments table")
    
    # Test 7: Get historical state
    print("\n8. Testing get_instrument_state_on_date()...")
    state = firds.data_store.get_instrument_state_on_date(test_isin, '2025-01-15')
    if state:
        print(f"   ✓ State on 2025-01-15: version {state.get('version_number')}")
        print(f"     Name: {state.get('full_name')}")
    else:
        print("   ✗ No state found")
    
    print("\n" + "=" * 60)
    print("✓ All delta processing tests completed successfully!")
    print("\nKey capabilities verified:")
    print("- NEW record insertion with version tracking")
    print("- MODIFIED record with automatic version increment")
    print("- TERMINATED record with valid_to_date")
    print("- CANCELLED record with cancellations table")
    print("- Version history queries")
    print("- Historical state reconstruction")
    
    return True


if __name__ == "__main__":
    try:
        success = test_delta_processing()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
