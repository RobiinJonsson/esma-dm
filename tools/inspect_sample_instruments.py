"""Inspect sample instruments from each asset type to verify database model completeness."""
import duckdb
from pathlib import Path
import json

# Connect to database
db_path = Path('esma_dm/config/storage/duckdb/database/firds_current.duckdb')
if not db_path.exists():
    print(f"Database not found at {db_path}")
    exit(1)

con = duckdb.connect(str(db_path))

print("=" * 80)
print("Sample Instrument Reference Data by Asset Type")
print("=" * 80)

# Define asset types and their table names
asset_types = {
    'C': 'civ_instruments',
    'D': 'debt_instruments',
    'E': 'equity_instruments',
    'F': 'futures_instruments',
    'H': 'rights_instruments',
    'I': 'spot_instruments',
    'J': 'forward_instruments',
    'O': 'option_instruments',
    'R': 'rights_instruments',  # R type goes to rights table
    'S': 'swap_instruments'
}

for asset_type, table_name in sorted(asset_types.items()):
    print(f"\n{'=' * 80}")
    print(f"Asset Type: {asset_type} ({table_name})")
    print('=' * 80)
    
    # Get one sample ISIN for this asset type
    query = f"""
        SELECT isin, cfi_code, full_name, short_name, issuer, currency, 
               competent_authority, publication_date
        FROM instruments 
        WHERE cfi_code LIKE '{asset_type}%'
        LIMIT 1
    """
    
    try:
        result = con.execute(query).fetchone()
        if not result:
            print(f"No instruments found for asset type {asset_type}")
            continue
        
        isin, cfi_code, full_name, short_name, issuer, currency, competent_authority, publication_date = result
        
        print(f"\nMain Instruments Table:")
        print(f"  ISIN: {isin}")
        print(f"  CFI Code: {cfi_code}")
        print(f"  Full Name: {full_name}")
        print(f"  Short Name: {short_name}")
        print(f"  Issuer: {issuer}")
        print(f"  Currency: {currency}")
        print(f"  Competent Authority: {competent_authority}")
        print(f"  Publication Date: {publication_date}")
        
        # Get data from asset-specific table
        asset_query = f"SELECT * FROM {table_name} WHERE isin = ?"
        asset_result = con.execute(asset_query, [isin]).fetchdf()
        
        if len(asset_result) > 0:
            print(f"\nAsset-Specific Table ({table_name}):")
            # Convert to dict for better display
            row = asset_result.iloc[0].to_dict()
            for key, value in sorted(row.items()):
                if key != 'isin':  # Skip ISIN as we already displayed it
                    # Format value for display
                    if value is None or (isinstance(value, float) and str(value) == 'nan'):
                        display_value = 'NULL'
                    else:
                        display_value = str(value)
                    print(f"  {key}: {display_value}")
        else:
            print(f"\nNo data found in {table_name} for ISIN {isin}")
        
        # Get listings count for this instrument
        listings_query = "SELECT COUNT(*) FROM listings WHERE isin = ?"
        listings_count = con.execute(listings_query, [isin]).fetchone()[0]
        print(f"\nListings: {listings_count} trading venues")
        
        if listings_count > 0:
            # Show sample listings
            sample_listings = con.execute(
                "SELECT trading_venue_id, first_trade_date, issuer_request FROM listings WHERE isin = ? LIMIT 3",
                [isin]
            ).fetchdf()
            print(f"  Sample venues: {', '.join(sample_listings['trading_venue_id'].tolist())}")
        
    except Exception as e:
        print(f"Error processing asset type {asset_type}: {e}")

print(f"\n{'=' * 80}")
print("Inspection Complete")
print('=' * 80)

con.close()
