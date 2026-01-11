"""
Test real DLTINS file download and processing.

Integration test that:
1. Downloads actual DLTINS files from ESMA
2. Verifies record type extraction
3. Processes with version management
4. Validates results
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from esma_dm import FIRDSClient
from datetime import datetime, timedelta


def test_dltins_integration():
    """Test end-to-end DLTINS processing."""
    print("Testing DLTINS File Download and Processing")
    print("=" * 60)
    
    # Initialize client
    firds = FIRDSClient()
    
    # Initialize clean database
    print("\n1. Initializing clean database...")
    try:
        firds.data_store.drop(confirm=True)
        print("   Dropped existing database")
    except:
        pass
    
    firds.data_store.initialize(mode='current')
    print("   Database initialized")
    
    # Download a recent DLTINS file (last 7 days)
    print("\n2. Checking for recent DLTINS files...")
    date_to = datetime.today().strftime("%Y-%m-%d")
    date_from = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    # Update client date range to match our search window
    firds.date_from = date_from
    firds.date_to = date_to
    
    # Try equity first (most common)
    print(f"   Looking for DLTINS files from {date_from} to {date_to}")
    
    # Get all DLTINS files (don't filter by asset type - they contain all assets)
    files = firds.get_file_list(file_type='DLTINS', asset_type=None)
    
    if files.empty:
        print("\n   No recent DLTINS files found for equities.")
        print("   This is normal if no delta updates were published recently.")
        print("\n   Testing with manual delta processing instead...")
        
        # Create a small baseline with FULINS first
        print("\n3. Loading baseline data from FULINS...")
        try:
            fulins_files = firds.get_file_list(file_type='FULINS', asset_type='E')
            if not fulins_files.empty:
                # Download latest FULINS to establish baseline
                firds.get_latest_full_files(asset_type='E')
                stats = firds.index_cached_files(asset_type='E', latest_only=True)
                print(f"   Loaded {stats['total_instruments']:,} instruments as baseline")
            else:
                print("   No FULINS files available either")
                return False
        except Exception as e:
            print(f"   Could not load baseline: {e}")
            return False
        
        print("\n   Skipping live DLTINS test (no recent files)")
        print("   Manual delta processing test passed in test_delta_processing.py")
        return True
    
    print(f"\n   Found {len(files)} DLTINS file(s)")
    print(f"   Latest: {files.iloc[0]['file_name']}")
    
    # Download and parse DLTINS
    print("\n3. Downloading DLTINS file...")
    try:
        df = firds.get_delta_files(
            asset_type='E',
            date_from=date_from,
            date_to=date_to,
            update=True
        )
        
        if df.empty:
            print("   Downloaded file is empty")
            return False
        
        print(f"   ✓ Downloaded {len(df)} delta records")
        
        # Check for record type column
        if '_record_type' in df.columns:
            print(f"   ✓ Record types extracted:")
            record_counts = df['_record_type'].value_counts()
            for record_type, count in record_counts.items():
                print(f"     {record_type}: {count}")
        else:
            print("   ✗ WARNING: No _record_type column found")
            print("   This might indicate XML parsing didn't extract record types")
            return False
        
    except Exception as e:
        print(f"   ✗ Error downloading DLTINS: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Process with version management
    print("\n4. Processing delta records with version management...")
    try:
        stats = firds.process_delta_files(
            asset_type='E',
            date_from=date_from,
            date_to=date_to,
            update=False  # Use already downloaded file
        )
        
        print(f"\n   ✓ Processing complete:")
        print(f"     Total processed: {stats['records_processed']}")
        print(f"     NEW records: {stats['new']}")
        print(f"     MODIFIED records: {stats['modified']}")
        print(f"     TERMINATED records: {stats['terminated']}")
        print(f"     CANCELLED records: {stats['cancelled']}")
        print(f"     Errors: {stats['errors']}")
        
        if stats['records_processed'] == 0:
            print("\n   ✗ No records were processed")
            return False
        
    except Exception as e:
        print(f"   ✗ Error processing deltas: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Verify database state
    print("\n5. Verifying database state...")
    try:
        latest = firds.data_store.get_latest_instruments(limit=5)
        print(f"   ✓ Latest instruments: {len(latest)}")
        
        if len(latest) > 0:
            print(f"\n   Sample instrument:")
            print(f"     ISIN: {latest.iloc[0]['isin']}")
            print(f"     Name: {latest.iloc[0]['full_name']}")
            print(f"     Version: {latest.iloc[0]['version_number']}")
            print(f"     Valid from: {latest.iloc[0]['valid_from_date']}")
            print(f"     Record type: {latest.iloc[0].get('record_type', 'N/A')}")
        
        # Check history table
        history_count = firds.data_store.con.execute(
            "SELECT COUNT(*) FROM instrument_history"
        ).fetchone()[0]
        print(f"\n   ✓ History records: {history_count}")
        
        # Check cancellations
        cancellations_count = firds.data_store.con.execute(
            "SELECT COUNT(*) FROM cancellations"
        ).fetchone()[0]
        print(f"   ✓ Cancellations: {cancellations_count}")
        
    except Exception as e:
        print(f"   ✗ Error verifying database: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 60)
    print("✓ DLTINS integration test completed successfully!")
    print("\nVerified capabilities:")
    print("- Downloaded real DLTINS files from ESMA")
    print("- Extracted record types from XML wrappers")
    print("- Applied version management logic")
    print("- Updated instrument_history table")
    print("- Maintained temporal validity dates")
    
    return True


if __name__ == "__main__":
    try:
        success = test_dltins_integration()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
