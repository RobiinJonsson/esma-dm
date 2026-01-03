"""
Test vectorized storage implementation.
"""

from pathlib import Path
import time
from esma_dm.storage.duckdb_store import DuckDBStorage

def test_vectorized_storage():
    """Test the new vectorized storage approach."""
    
    # Initialize storage
    cache_dir = Path('downloads')
    db_path = str(cache_dir / 'data' / 'firds' / 'firds.db')
    
    # Remove old database to start fresh
    if Path(db_path).exists():
        Path(db_path).unlink()
        print(f"Removed existing database: {db_path}")
    
    storage = DuckDBStorage(cache_dir, db_path)
    
    # Test with multiple FULINS files
    test_files = [
        'downloads/data/firds/FULINS_F_20240601_01of01_data.csv',  # Futures - 48K
        'downloads/data/firds/FULINS_D_20250510_01of03_data.csv',  # Debt - 500K
        'downloads/data/firds/FULINS_E_20250510_01of02_data.csv',  # Equities - 500K
    ]
    
    total_start = time.time()
    
    for file_path in test_files:
        path = Path(file_path)
        if not path.exists():
            print(f"File not found: {file_path}")
            continue
        
        print(f"\n{'='*60}")
        print(f"Testing: {path.name}")
        print(f"{'='*60}")
        
        start = time.time()
        count = storage.index_csv_file(str(path))
        elapsed = time.time() - start
        
        if count > 0:
            rate = count / elapsed
            print(f"✓ Indexed {count:,} instruments in {elapsed:.2f}s ({rate:,.0f} inst/sec)")
        else:
            print("Already indexed or no instruments")
    
    total_elapsed = time.time() - total_start
    
    # Show statistics
    print(f"\n{'='*60}")
    print("Asset Type Statistics")
    print(f"{'='*60}")
    stats = storage.get_stats_by_asset_type()
    print(stats.to_string(index=False))
    
    print(f"\n{'='*60}")
    print(f"Total time: {total_elapsed:.2f}s")
    print(f"{'='*60}")
    
    # Test querying an instrument
    print(f"\n{'='*60}")
    print("Testing instrument query...")
    print(f"{'='*60}")
    
    # Get first instrument from database
    result = storage.con.execute("SELECT isin FROM instruments LIMIT 1").fetchone()
    if result:
        isin = result[0]
        instrument = storage.get_instrument(isin)
        
        print(f"\nInstrument: {isin}")
        print(f"Type: {instrument.get('instrument_type')}")
        print(f"Name: {instrument.get('full_name')}")
        print(f"CFI: {instrument.get('cfi_code')}")
        print(f"Currency: {instrument.get('currency')}")
        
        # Show type-specific fields
        if instrument.get('instrument_type') == 'E':
            print(f"Dividend Frequency: {instrument.get('dividend_payment_frequency')}")
            print(f"Voting Rights: {instrument.get('voting_rights_per_share')}")
        elif instrument.get('instrument_type') == 'D':
            print(f"Maturity: {instrument.get('maturity_date')}")
            print(f"Interest Rate Type: {instrument.get('interest_rate_type')}")
        elif instrument.get('instrument_type') in ['F', 'O', 'S']:
            print(f"Expiry: {instrument.get('expiry_date')}")
            print(f"Underlying: {instrument.get('underlying_isin')}")
    
    storage.close()
    print("\n✓ Test complete")

if __name__ == '__main__':
    test_vectorized_storage()
