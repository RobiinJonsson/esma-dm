"""
Example: Using the reference API to discover available subtypes.

Demonstrates how to query available subtype output models and their properties.
"""

import esma_dm as edm

# Check if database has data
try:
    firds = edm.FIRDSClient()
    count = firds.data_store.con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
    if count == 0:
        print("\nDatabase is empty. Please run:")
        print("  1. python examples/00_initialize_database.py")
        print("  2. Download and index data (see examples/02_index_with_filters.py)")
        exit(1)
except Exception as e:
    print(f"\nDatabase error: {e}")
    print("\nPlease run:")
    print("  python examples/00_initialize_database.py")
    exit(1)

print("="*80)
print("AVAILABLE SUBTYPE OUTPUT MODELS")
print("="*80)

# Get all available subtypes
subtypes = edm.reference.subtypes()

print(f"\nTotal subtype models available: {len(subtypes)}")
print(f"\nSubtypes ordered by volume:\n")
print(subtypes.to_string(index=False))

print("\n" + "="*80)
print("SUBTYPE USAGE EXAMPLES")
print("="*80)

print("""
# Query instruments and get typed subtype instances:

from esma_dm.models import parse_instrument

# Example 1: Equity Swaps (SE*)
equity_swaps = firds.data_store.query(\"\"\"
    SELECT * FROM swaps 
    WHERE cfi_code LIKE 'SE%' 
    LIMIT 10
\"\"\")

for row in equity_swaps:
    swap = parse_instrument(row)  # Returns EquitySwap instance
    print(f"Underlying: {swap.underlying_index_name}")
    print(f"Rate: {swap.interest_rate_reference_name}")
    

# Example 2: Swaptions (HR*)  
swaptions = firds.data_store.query(\"\"\"
    SELECT * FROM non_standardized
    WHERE cfi_code LIKE 'HR%'
    LIMIT 10
\"\"\")

for row in swaptions:
    swaption = parse_instrument(row)  # Returns Swaption instance
    print(f"Option Type: {swaption.option_type}")
    print(f"Strike: {swaption.strike_price_amount}")
    print(f"Fixed Rate: {swaption.first_leg_interest_rate_fixed}")


# Example 3: Mini-Futures (RF*)
mini_futures = firds.data_store.query(\"\"\"
    SELECT * FROM entitlements
    WHERE cfi_code LIKE 'RF%'
    LIMIT 10
\"\"\")

for row in mini_futures:
    mf = parse_instrument(row)  # Returns MiniFuture instance
    print(f"Underlying: {mf.underlying_index_name}")
    print(f"Option Type: {mf.option_type}")
    print(f"Strike: {mf.strike_price_amount}")
    if mf.commodity_metal_precious_base:
        print(f"Commodity: {mf.commodity_metal_precious_base}")


# Example 4: Commodity Futures (FC*)
from esma_dm.models import CommodityFuture

commodity_futures = firds.data_store.query(\"\"\"
    SELECT * FROM futures
    WHERE cfi_code LIKE 'FC%'
    LIMIT 10
\"\"\")

for row in commodity_futures:
    future = CommodityFuture.from_dict(row)
    print(f"Commodity: {future.commodity_dairy_base or future.commodity_grain_base}")
    print(f"Transaction Type: {future.commodity_transaction_type}")
    print(f"Final Price Type: {future.commodity_final_price_type}")
""")

print("\n" + "="*80)
print("ARCHITECTURE SUMMARY")
print("="*80)

print("""
STORAGE (Database):
  • Common columns (isin, cfi_code, name, dates, issuer, etc.)
  • JSONB 'attributes' column with 22-56 subtype-specific FIRDS fields

OUTPUT (Python Models):
  • Typed dataclasses per CFI subtype (8 models covering 12.4M instruments)
  • from_dict() method parses JSON and maps FIRDS fields to properties
  • Based on actual verified FIRDS field names
  • IDE autocomplete and type hints

BENEFITS:
  ✓ Efficient storage (no NULL columns for unused fields)
  ✓ Complete data coverage (all FIRDS fields extracted)
  ✓ Typed Python access (autocomplete, validation)
  ✓ Easy to query common fields (indexed columns)
  ✓ Flexible JSON queries for specialized fields
  ✓ Extensible (new subtypes without schema changes)
""")

if __name__ == "__main__":
    # Actually run the subtypes query if esma_dm is properly configured
    try:
        print("\n" + "="*80)
        print("ACTUAL DATA FROM YOUR INSTALLATION")
        print("="*80)
        subtypes_actual = edm.reference.subtypes()
        print(subtypes_actual.to_string(index=False))
    except Exception as e:
        print(f"\nNote: Could not query actual data: {e}")
        print("This is normal if database hasn't been initialized yet.")
