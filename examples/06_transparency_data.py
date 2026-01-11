"""
Example: FITRS Transparency Data Access
========================================

Demonstrates how to:
- Index transparency data (FULECR and FULNCR)
- Query transparency metrics by ISIN
- Filter by liquidity and turnover
- Perform cross-database queries with FIRDS data
"""

from esma_dm import transparency, reference

# Example 1: Index equity transparency data
print("=" * 60)
print("Indexing Equity Transparency Data (FULECR)")
print("=" * 60)

result = transparency.index('FULECR', latest_only=True)
print(f"Status: {result['status']}")
print(f"Files processed: {result['files_processed']}")
print(f"Total instruments: {result['total_instruments']}")
print(f"Instrument type: {result['instrument_type']}")
print()

# Example 2: Index non-equity transparency data
print("=" * 60)
print("Indexing Non-Equity Transparency Data (FULNCR)")
print("=" * 60)

result = transparency.index('FULNCR', latest_only=True)
print(f"Status: {result['status']}")
print(f"Files processed: {result['files_processed']}")
print(f"Total instruments: {result['total_instruments']}")
print(f"Instrument type: {result['instrument_type']}")
print()

# Example 3: Look up transparency for specific ISIN
print("=" * 60)
print("Lookup Transparency by ISIN")
print("=" * 60)

isin = 'GB00B1YW4409'  # Example ISIN
trans_data = transparency(isin)

if trans_data:
    print(f"ISIN: {trans_data['isin']}")
    print(f"Liquid Market: {trans_data['liquid_market']}")
    print(f"Average Daily Turnover: {trans_data.get('average_daily_turnover')}")
    print(f"Instrument Type: {trans_data['instrument_type']}")
else:
    print(f"No transparency data found for {isin}")
print()

# Example 4: Query liquid instruments with high turnover
print("=" * 60)
print("Query Liquid Instruments (Turnover > 1M)")
print("=" * 60)

liquid_df = transparency.query(
    liquid_only=True,
    min_turnover=1_000_000,
    limit=10
)

print(f"Found {len(liquid_df)} liquid instruments")
print(liquid_df[['isin', 'liquid_market', 'average_daily_turnover', 'instrument_type']].head())
print()

# Example 5: Query by instrument type
print("=" * 60)
print("Query Equity Transparency Only")
print("=" * 60)

equity_df = transparency.query(
    instrument_type='equity',
    limit=10
)

print(f"Found {len(equity_df)} equity instruments")
print(equity_df[['isin', 'liquid_market', 'average_daily_turnover']].head())
print()

# Example 6: Cross-database query with FIRDS
print("=" * 60)
print("Cross-Database Query: FIRDS + FITRS")
print("=" * 60)

# Attach FIRDS database for combined queries
transparency.attach_firds()

# Query combining reference data and transparency metrics
sql = """
SELECT 
    f.isin,
    f.full_name,
    f.cfi_code,
    t.liquid_market,
    t.average_daily_turnover,
    t.instrument_type
FROM firds.instruments f
JOIN transparency t ON f.isin = t.isin
WHERE t.liquid_market = 'Y'
  AND t.average_daily_turnover > 5000000
ORDER BY t.average_daily_turnover DESC
LIMIT 10
"""

combined_df = transparency.client.data_store.query(sql)

print(f"Found {len(combined_df)} liquid instruments with reference data")
print(combined_df[['isin', 'full_name', 'liquid_market', 'average_daily_turnover']].head())
print()

# Example 7: Analyze transparency by asset type
print("=" * 60)
print("Transparency Analysis by Asset Type")
print("=" * 60)

sql = """
SELECT 
    SUBSTR(f.cfi_code, 1, 1) as asset_type,
    COUNT(*) as total_instruments,
    SUM(CASE WHEN t.liquid_market = 'Y' THEN 1 ELSE 0 END) as liquid_count,
    AVG(t.average_daily_turnover) as avg_turnover,
    MAX(t.average_daily_turnover) as max_turnover
FROM firds.instruments f
JOIN transparency t ON f.isin = t.isin
WHERE f.cfi_code IS NOT NULL
GROUP BY SUBSTR(f.cfi_code, 1, 1)
ORDER BY total_instruments DESC
"""

analysis_df = transparency.client.data_store.query(sql)

print("Asset Type Distribution:")
print(analysis_df)
print()

# Example 8: Find illiquid derivatives
print("=" * 60)
print("Find Illiquid Derivatives")
print("=" * 60)

sql = """
SELECT 
    f.isin,
    f.full_name,
    f.cfi_code,
    t.liquid_market,
    t.average_daily_turnover
FROM firds.instruments f
JOIN transparency t ON f.isin = t.isin
WHERE SUBSTR(f.cfi_code, 1, 1) IN ('F', 'O', 'H', 'J', 'K', 'L', 'M', 'T')
  AND t.liquid_market = 'N'
LIMIT 10
"""

illiquid_df = transparency.client.data_store.query(sql)

print(f"Found {len(illiquid_df)} illiquid derivatives")
if not illiquid_df.empty:
    print(illiquid_df[['isin', 'cfi_code', 'liquid_market']].head())
print()

print("=" * 60)
print("Transparency API Examples Complete")
print("=" * 60)
