"""
Example: FITRS Transparency Data

Demonstrates how to index, query, and analyze FITRS transparency results.
Both FIRDS reference data and FITRS transparency data share the same unified
database (esma_{mode}.duckdb) -- no cross-database ATTACH required.
"""

import esma_dm as edm
from esma_dm import FITRSClient


def main():
    print("ESMA Data Manager - Transparency Data")
    print("=" * 60)

    # 1. Index equity transparency data (FULECR)
    print("\n1. Index Equity Transparency Data (FULECR)")
    print("-" * 60)
    fitrs = FITRSClient()
    _eq_ok = fitrs.data_store.con.execute(
        "SELECT COUNT(*) FROM transparency WHERE instrument_type = 'equity' AND liquid_market IS NOT NULL"
    ).fetchone()[0]
    if _eq_ok > 0:
        _eq_total = fitrs.data_store.con.execute(
            "SELECT COUNT(*) FROM transparency WHERE instrument_type = 'equity'"
        ).fetchone()[0]
        print(f"  Already indexed: {_eq_total:,} equity records -- skipping download")
    else:
        result = edm.transparency.index('FULECR', latest_only=True)
        print(f"  Status          : {result.get('status')}")
        print(f"  Files processed : {result.get('files_processed')}")
        print(f"  Records loaded  : {result.get('total_records', 0):,}")

    # 2. Index non-equity transparency data (FULNCR)
    print("\n2. Index Non-Equity Transparency Data (FULNCR)")
    print("-" * 60)
    _neq_ok = fitrs.data_store.con.execute(
        "SELECT COUNT(*) FROM transparency WHERE instrument_type = 'non_equity' AND liquid_market IS NOT NULL"
    ).fetchone()[0]
    if _neq_ok > 0:
        _neq_total = fitrs.data_store.con.execute(
            "SELECT COUNT(*) FROM transparency WHERE instrument_type = 'non_equity'"
        ).fetchone()[0]
        print(f"  Already indexed: {_neq_total:,} non-equity records -- skipping download")
    else:
        result = edm.transparency.index('FULNCR', latest_only=True)
        print(f"  Status          : {result.get('status')}")
        print(f"  Files processed : {result.get('files_processed')}")
        print(f"  Records loaded  : {result.get('total_records', 0):,}")

    # 2b. Index non-equity sub-class data (FULNCR_NYAR)
    print("\n2b. Index Non-Equity Sub-Class Data (FULNCR_NYAR)")
    print("-" * 60)
    _nyar_count = fitrs.data_store.con.execute(
        "SELECT COUNT(*) FROM subclass_transparency"
    ).fetchone()[0]
    if _nyar_count > 0:
        print(f"  Already indexed: {_nyar_count:,} sub-class records -- skipping download")
    else:
        result = edm.transparency.index('FULNCR_NYAR', latest_only=True)
        print(f"  Status          : {result.get('status')}")
        print(f"  Files processed : {result.get('files_processed')}")
        print(f"  Records loaded  : {result.get('total_records', 0):,}")

    # 3. Direct ISIN lookup
    print("\n3. Transparency Lookup by ISIN")
    print("-" * 60)
    isin = 'GB00B1YW4409'
    trans = edm.transparency(isin)
    if trans:
        print(f"  ISIN                  : {trans.get('isin')}")
        print(f"  Liquid market         : {trans.get('liquid_market')}")
        print(f"  Average daily turnover: {trans.get('average_daily_turnover')}")
        print(f"  Methodology           : {trans.get('methodology')}")
        print(f"  Instrument type       : {trans.get('instrument_type')}")
    else:
        print(f"  No transparency data found for {isin}")

    # 4. Query liquid instruments with high turnover
    print("\n4. Liquid Instruments (Turnover > 1M)")
    print("-" * 60)
    liquid_df = edm.transparency.query(
        liquid_only=True,
        min_turnover=1_000_000,
        limit=10
    )
    print(f"  Found: {len(liquid_df)} instruments")
    if not liquid_df.empty:
        cols = [c for c in ('isin', 'liquid_market', 'average_daily_turnover',
                            'instrument_type') if c in liquid_df.columns]
        print(liquid_df[cols].to_string(index=False))

    # 5. Sub-class transparency -- show first available asset class
    print("\n5. Sub-Class Transparency")
    print("-" * 60)
    _available = fitrs.data_store.con.execute(
        "SELECT DISTINCT asset_class FROM subclass_transparency ORDER BY asset_class LIMIT 5"
    ).fetchdf()
    if _available.empty:
        print("  No sub-class records indexed yet.")
    else:
        first_class = _available.iloc[0]['asset_class']
        print(f"  Available asset classes (sample): {', '.join(_available['asset_class'].tolist())}")
        subclass_df = edm.transparency.query_subclass(asset_class=first_class, limit=5)
        print(f"  Sample -- asset class '{first_class}': {len(subclass_df)} records")
        if not subclass_df.empty:
            print(subclass_df.head(5).to_string(index=False))

    # 6. Methodology reference codes
    print("\n6. Available Methodologies")
    print("-" * 60)
    fitrs = FITRSClient()
    for m in fitrs.list_methodologies():
        print(f"  {m['code']:6} : {m['description']}")

    # 7. Cross-table query in the unified database (no ATTACH needed)
    print("\n7. Cross-Table Query: Instruments + Transparency (Unified DB)")
    print("-" * 60)
    firds = edm.FIRDSClient()
    df = firds.query_database("""
        SELECT
            i.isin,
            i.full_name,
            i.cfi_code,
            t.liquid_market,
            t.average_daily_turnover,
            t.methodology
        FROM instruments i
        JOIN transparency t ON i.isin = t.isin
        WHERE t.liquid_market = TRUE
          AND t.average_daily_turnover > 5000000
        ORDER BY t.average_daily_turnover DESC
        LIMIT 10
    """)
    print(f"  Found: {len(df)} liquid instruments with reference data")
    if not df.empty:
        print(df[['isin', 'full_name', 'average_daily_turnover']].head(5).to_string(
            index=False
        ))

    # 8. Liquidity rate by CFI category
    print("\n8. Liquidity Rate by CFI Category")
    print("-" * 60)
    df2 = firds.query_database("""
        SELECT
            SUBSTR(i.cfi_code, 1, 1)                               AS asset_type,
            COUNT(*)                                                AS total,
            SUM(CASE WHEN t.liquid_market = TRUE THEN 1 ELSE 0 END) AS liquid,
            ROUND(AVG(t.average_daily_turnover), 0)                AS avg_turnover
        FROM instruments i
        JOIN transparency t ON i.isin = t.isin
        WHERE i.cfi_code IS NOT NULL
        GROUP BY SUBSTR(i.cfi_code, 1, 1)
        ORDER BY total DESC
    """)
    print(df2.to_string(index=False))


if __name__ == "__main__":
    main()
