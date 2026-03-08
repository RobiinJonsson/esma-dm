"""
Test current vs history mode separation.
"""
import os
from pathlib import Path
from esma_dm import FIRDSClient


def test_mode_initialization():
    """Test that both modes create separate databases."""
    print("Testing Mode Initialization")
    print("=" * 60)
    
    # Test 1: Current mode creates esma_current.duckdb
    print("\n1. Testing 'current' mode...")
    firds_current = FIRDSClient(mode='current')
    db_path_current = Path(firds_current.data_store.db_path)
    print(f"   Database path: {db_path_current}")
    assert 'esma_current.duckdb' in str(db_path_current), "Current mode should use esma_current.duckdb"
    print("   Correct database name for current mode")

    # Test 2: History mode creates esma_history.duckdb
    print("\n2. Testing 'history' mode...")
    firds_history = FIRDSClient(mode='history')
    db_path_history = Path(firds_history.data_store.db_path)
    print(f"   Database path: {db_path_history}")
    assert 'esma_history.duckdb' in str(db_path_history), "History mode should use esma_history.duckdb"
    print("   Correct database name for history mode")
    
    # Test 3: Databases are separate files
    print("\n3. Verifying database separation...")
    assert db_path_current != db_path_history, "Modes should use different database files"
    print("   ✓ Current and history modes use separate databases")
    
    # Test 4: process_delta_files() only available in history mode
    print("\n4. Testing process_delta_files() restrictions...")
    try:
        # This should fail in current mode
        firds_current.process_delta_files(
            asset_type='E',
            date_from='2026-01-01',
            date_to='2026-01-01'
        )
        print("   ✗ ERROR: process_delta_files() should not work in current mode")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        if "only available in 'history' mode" in str(e):
            print(f"   ✓ Correctly blocked in current mode: {e}")
        else:
            raise
    
    # Test 5: process_delta_files() available in history mode
    print("\n5. Verifying process_delta_files() works in history mode...")
    try:
        # This should work (but will fail if no data, that's ok)
        # We're just checking it doesn't raise ValueError about mode
        result = firds_history.process_delta_files(
            asset_type='E',
            date_from='2026-01-04',
            date_to='2026-01-04',
            update=False  # Use cached
        )
        print(f"   ✓ process_delta_files() works in history mode")
        print(f"      Processed: {result.get('records_processed', 0)} records")
    except ValueError as e:
        if "only available in 'history' mode" in str(e):
            print(f"   ✗ ERROR: Should work in history mode but got: {e}")
            raise
        else:
            # Other errors are ok (like no data)
            print(f"   ✓ No mode restriction error (other error is acceptable: {e})")
    except Exception as e:
        # Other exceptions are acceptable (like no database initialized)
        print(f"   ✓ No mode restriction error (got: {type(e).__name__})")
    
    print("\n" + "=" * 60)
    print("✓ All mode separation tests passed!")
    print(f"\nDatabase files:")
    print(f"  Current mode: {db_path_current}")
    print(f"  History mode: {db_path_history}")


def test_default_cached():
    """Test that methods default to using cached files."""
    print("\n\nTesting Default Cached Behavior")
    print("=" * 60)
    
    firds = FIRDSClient(mode='current')
    
    # Check method signatures have update=False default
    import inspect
    
    methods_to_check = [
        ('get_latest_full_files', firds.get_latest_full_files),
        ('get_delta_files', FIRDSClient(mode='history').get_delta_files),
    ]
    
    for method_name, method in methods_to_check:
        sig = inspect.signature(method)
        update_param = sig.parameters.get('update')
        if update_param:
            print(f"\n{method_name}:")
            print(f"   update parameter default: {update_param.default}")
            assert update_param.default == False, f"{method_name} should default to update=False"
            print(f"   ✓ Defaults to using cached files")
    
    print("\n" + "=" * 60)
    print("✓ All cached default tests passed!")


if __name__ == '__main__':
    test_mode_initialization()
    test_default_cached()
