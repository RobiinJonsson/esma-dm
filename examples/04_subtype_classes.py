"""
Example: Using subtype classes to access detailed instrument attributes.

This demonstrates the hybrid architecture where:
1. Common fields are stored as columns in the database
2. Subtype-specific fields are stored in JSONB 'attributes' column
3. Python classes parse the JSON and provide typed property access
"""

import esma_dm as edm

# Initialize FIRDS client
firds = edm.FIRDSClient()

# Check if database has data
try:
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

# Example 1: Query rate swaps (SR*) and access detailed attributes
print("="*80)
print("EXAMPLE 1: Rate Swaps (Interest Rate Swaps)")
print("="*80)

# Query from database (hypothetical - database needs JSONB support first)
# rate_swaps = firds.data_store.query("SELECT * FROM swaps WHERE cfi_code LIKE 'SR%' LIMIT 5")

# For now, demonstrate with mock data showing the structure
rate_swap_data = {
    'isin': 'XS1234567890',
    'cfi_code': 'SRACCC',
    'full_name': 'EUR 5Y Interest Rate Swap',
    'asset_type': 'S',
    'issuer_lei': 'ABCDEF1234567890',
    'maturity_date': '2030-01-15',
    'notional_currency_1': 'EUR',
    # JSONB attributes column contains subtype-specific fields
    'attributes': {
        'fixed_rate': 3.5,
        'floating_rate_index': 'EURIBOR',
        'floating_rate_term': '6M',
        'day_count_convention': 'ACT/360',
        'payment_frequency_fixed': 'Annual',
        'payment_frequency_floating': 'Semi-annual',
        'reset_frequency': 'Semi-annual',
        'spread': 0.25,
        'settlement_currency': 'EUR'
    }
}

# Parse into typed subtype class
rate_swap = edm.models.RateSwap(**rate_swap_data)
print(f"\nISIN: {rate_swap.isin}")
print(f"Name: {rate_swap.full_name}")
print(f"Fixed Rate: {rate_swap.fixed_rate}%")
print(f"Floating Rate: {rate_swap.floating_rate_index} {rate_swap.floating_rate_term}")
print(f"Day Count: {rate_swap.day_count_convention}")
print(f"Payment Freq: Fixed={rate_swap.payment_frequency_fixed}, Float={rate_swap.payment_frequency_floating}")

# Example 2: Mini-Future Certificates (RF*) - most common entitlement
print("\n" + "="*80)
print("EXAMPLE 2: Mini-Future Certificates (Leveraged Products)")
print("="*80)

mini_future_data = {
    'isin': 'DE000ABC1234',
    'cfi_code': 'RFBTCB',
    'full_name': 'Mini-Future Long on DAX',
    'asset_type': 'R',
    'issuer_lei': 'BANK1234567890AB',
    'maturity_date': '2026-12-31',
    'attributes': {
        'leverage_factor': 10.0,
        'financing_level': 18500.50,
        'stop_loss_level': 18450.00,
        'knock_out_level': 18450.00,
        'direction': 'LONG',
        'adjustment_interval': 'Daily',
        'underlying_type': 'INDEX',
        'underlying_isin': 'DE0008469008',
        'underlying_name': 'DAX',
        'underlying_currency': 'EUR',
        'management_fee': 0.015,
        'financing_spread': 0.025
    }
}

mini_future = edm.models.MiniFuture(**mini_future_data)
print(f"\nISIN: {mini_future.isin}")
print(f"Product: {mini_future.full_name}")
print(f"Direction: {mini_future.direction}")
print(f"Leverage: {mini_future.leverage_factor}x")
print(f"Financing Level: {mini_future.financing_level}")
print(f"Stop Loss: {mini_future.stop_loss_level}")
print(f"Underlying: {mini_future.underlying_name} ({mini_future.underlying_isin})")
print(f"Fees: Management={mini_future.management_fee}%, Financing Spread={mini_future.financing_spread}%")

# Example 3: Swaptions (HR*) - interest rate options
print("\n" + "="*80)
print("EXAMPLE 3: Swaptions (Options on Interest Rate Swaps)")
print("="*80)

swaption_data = {
    'isin': 'US9876543210',
    'cfi_code': 'HRAGVC',
    'full_name': '2Y into 10Y EUR Payer Swaption',
    'asset_type': 'H',
    'issuer_lei': 'DEALER1234567890',
    'expiry_date': '2028-03-15',
    'attributes': {
        'option_type': 'CALL',
        'strike_rate': 4.25,
        'option_style': 'EURO',
        'premium': 150000,
        'premium_currency': 'EUR',
        'swap_tenor': '10Y',
        'fixed_rate': 4.25,
        'floating_rate_index': 'EURIBOR',
        'floating_rate_term': '6M',
        'day_count_convention': 'ACT/360',
        'payment_frequency': 'Semi-annual'
    }
}

swaption = edm.models.Swaption(**swaption_data)
print(f"\nISIN: {swaption.isin}")
print(f"Product: {swaption.full_name}")
print(f"Option Type: {swaption.option_type}")
print(f"Strike Rate: {swaption.strike_rate}%")
print(f"Style: {swaption.option_style}")
print(f"Premium: {swaption.premium} {swaption.premium_currency}")
print(f"Underlying Swap: {swaption.swap_tenor} {swaption.floating_rate_index}")

# Example 4: Structured Equities (EY*) - most common equity subtype
print("\n" + "="*80)
print("EXAMPLE 4: Structured Equities (Participation Certificates)")
print("="*80)

structured_equity_data = {
    'isin': 'CH1234567890',
    'cfi_code': 'EYADFM',
    'full_name': 'Tracker Certificate on S&P 500',
    'asset_type': 'E',
    'issuer_lei': 'ISSUER1234567890',
    'maturity_date': '2030-06-30',
    'attributes': {
        'product_type': 'TRACKER',
        'participation_rate': 100.0,
        'cap_level': None,
        'floor_level': None,
        'barrier_level': None,
        'underlying_type': 'INDEX',
        'underlying_isin': 'US78378X1072',
        'underlying_name': 'S&P 500 Index',
        'underlying_currency': 'USD',
        'management_fee': 0.50,
        'redemption_type': 'CASH'
    }
}

structured_equity = edm.models.StructuredEquity(**structured_equity_data)
print(f"\nISIN: {structured_equity.isin}")
print(f"Product: {structured_equity.full_name}")
print(f"Type: {structured_equity.product_type}")
print(f"Participation: {structured_equity.participation_rate}%")
print(f"Underlying: {structured_equity.underlying_name}")
print(f"Management Fee: {structured_equity.management_fee}%")

# Example 5: Commodity Futures (FC*)
print("\n" + "="*80)
print("EXAMPLE 5: Commodity Futures")
print("="*80)

commodity_future_data = {
    'isin': 'US1234567890',
    'cfi_code': 'FCACSX',
    'full_name': 'WTI Crude Oil Future Dec 2026',
    'asset_type': 'F',
    'expiry_date': '2026-12-15',
    'attributes': {
        'commodity_base': 'ENERGY',
        'commodity_details': 'OIL',
        'commodity_description': 'West Texas Intermediate Crude',
        'delivery_point_or_zone': 'Cushing, Oklahoma',
        'energy_quantity_unit': 'BBL',
        'energy_settlement_method': 'PHYSICAL',
        'delivery_interval_start': '2026-12-01',
        'delivery_interval_end': '2026-12-31'
    }
}

commodity_future = edm.models.CommodityFuture(**commodity_future_data)
print(f"\nISIN: {commodity_future.isin}")
print(f"Contract: {commodity_future.full_name}")
print(f"Commodity: {commodity_future.commodity_description}")
print(f"Delivery: {commodity_future.delivery_point_or_zone}")
print(f"Settlement: {commodity_future.energy_settlement_method}")
print(f"Delivery Period: {commodity_future.delivery_interval_start} to {commodity_future.delivery_interval_end}")

print("\n" + "="*80)
print("ARCHITECTURE BENEFITS")
print("="*80)
print("""
The hybrid JSONB approach provides:

1. EFFICIENT STORAGE
   - Common fields (ISIN, CFI, name, dates) stored as columns for fast filtering
   - Subtype-specific fields (39 commodity fields, rate swap legs, etc.) in JSON
   - No NULL columns for attributes that don't apply to instrument type

2. FLEXIBLE QUERYING
   - SQL queries on common columns remain fast with proper indexes
   - JSON queries available for subtype-specific filtering when needed
   - Example: SELECT * FROM swaps WHERE cfi_code LIKE 'SR%' AND attributes->>'fixed_rate' > '3.0'

3. TYPED PYTHON ACCESS
   - Subtype classes provide IDE autocomplete and type hints
   - Complex attributes grouped logically (option attrs, swap legs, etc.)
   - No need to remember 70 different column names

4. EXTENSIBILITY
   - New instrument subtypes easily added with new subtype classes
   - No schema migrations needed for new subtype-specific fields
   - Backward compatible - old code still works with base classes

5. DATA COVERAGE
   - Can now extract ALL 70 fields from FIRDS data
   - No information loss - complete data preservation
   - Structured query access when needed, convenient object access always
""")
