"""
Example: Reference API - Queries by Asset Type

Demonstrates how to query instruments by asset type using the hierarchical
reference API. Each asset type exposes .count(), .types(), and .sample().
"""

import esma_dm as edm


def main():
    print("ESMA Data Manager - Reference API by Asset Type")
    print("=" * 60)

    firds = edm.FIRDSClient()
    try:
        count = firds.data_store.con.execute(
            "SELECT COUNT(*) FROM instruments"
        ).fetchone()[0]
        if count == 0:
            print("\nDatabase is empty. Run:")
            print("  firds.build_reference_database()")
            return
    except Exception:
        print("\nDatabase not initialized. Run:")
        print("  firds.initialize_database()")
        return

    # 1. Global summary
    print("\n1. Global Summary Across All Asset Types")
    print("-" * 60)
    summary = edm.reference.summary()
    print(summary.to_string(index=False))

    # 2. Equity instruments
    print("\n2. Equity Instruments")
    print("-" * 60)
    print(f"  Total equities  : {edm.reference.equity.count():,}")
    print("\n  CFI code distribution (top 10):")
    equity_types = edm.reference.equity.types()
    print(equity_types.head(10).to_string(index=False))
    print("\n  Sample equities:")
    equity_sample = edm.reference.equity.sample(5)
    print(equity_sample.to_string(index=False))

    # 3. Debt instruments
    print("\n3. Debt Instruments")
    print("-" * 60)
    print(f"  Total debt instruments: {edm.reference.debt.count():,}")
    debt_sample = edm.reference.debt.sample(5)
    print(debt_sample.to_string(index=False))

    # 4. Swap instruments
    print("\n4. Swap Instruments")
    print("-" * 60)
    print(f"  Total swaps : {edm.reference.swap.count():,}")
    print("\n  Swap CFI types (top 10):")
    swap_types = edm.reference.swap.types()
    print(swap_types.head(10).to_string(index=False))
    if len(swap_types) > 10:
        print(f"  ... and {len(swap_types) - 10} more swap CFI codes")

    # 5. Direct ISIN lookup
    print("\n5. Direct ISIN Lookup")
    print("-" * 60)
    row = firds.data_store.con.execute(
        "SELECT isin FROM instruments LIMIT 1"
    ).fetchone()
    if row:
        isin = row[0]
        instrument = edm.reference(isin)
        if instrument is not None:
            print(f"  edm.reference('{isin}'):")
            for key in ('isin', 'full_name', 'cfi_code', 'currency', 'issuer_lei'):
                val = instrument.get(key)
                if val:
                    print(f"    {key:<20}: {str(val)[:60]}")

    # 6. All CFI codes across every asset type
    print("\n6. All CFI Codes Across Asset Types")
    print("-" * 60)
    all_types = edm.reference.types()
    print(all_types.head(20).to_string(index=False))
    if len(all_types) > 20:
        print(f"  ... and {len(all_types) - 20} more unique CFI codes")

    # 7. Custom SQL via query_database
    print("\n7. Custom SQL: Currency Distribution by Asset Category")
    print("-" * 60)
    df = firds.query_database("""
        SELECT
            SUBSTR(cfi_code, 1, 1) AS category,
            currency,
            COUNT(*)               AS count
        FROM instruments
        WHERE currency IS NOT NULL
        GROUP BY category, currency
        ORDER BY count DESC
        LIMIT 10
    """)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
