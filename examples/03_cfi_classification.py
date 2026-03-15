"""
Example: CFI Classification

Demonstrates ISO 10962 CFI decoding and instrument classification across all
14 FIRDS asset categories using the reference API and CFI decode utilities.
"""

import esma_dm as edm
from esma_dm.models.utils.cfi.cfi_instrument_manager import decode_cfi

CATEGORIES = {
    'C': 'Collective Investment Vehicles',
    'D': 'Debt Instruments',
    'E': 'Equities',
    'F': 'Futures',
    'H': 'Non-Standard Derivatives',
    'I': 'Spot',
    'J': 'Forwards',
    'K': 'Strategies',
    'L': 'Financing',
    'M': 'Others',
    'O': 'Options',
    'R': 'Rights / Entitlements',
    'S': 'Swaps',
    'T': 'Referential',
}


def main():
    print("ESMA Data Manager - CFI Classification")
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

    # 1. Distribution across all 14 CFI categories
    print(f"\n1. Instrument Distribution Across All 14 CFI Categories")
    print("-" * 60)
    breakdown = firds.get_asset_breakdown()
    print(breakdown.to_string(index=False))

    # 2. Three decoding examples — one from Debt, Futures, Swaps
    print("\n2. CFI Decoding Examples (D, F, S)")
    print("-" * 60)
    for asset_letter, label in [('D', 'Debt'), ('F', 'Futures'), ('S', 'Swaps')]:
        row = firds.data_store.con.execute(f"""
            SELECT isin, full_name, cfi_code
            FROM instruments
            WHERE cfi_code LIKE '{asset_letter}%'
              AND LENGTH(cfi_code) = 6
            LIMIT 1
        """).fetchone()
        if not row:
            print(f"  [{label}] no instruments found")
            continue
        isin, name, cfi_code = row
        decoded = decode_cfi(cfi_code)
        print(f"\n  [{label}]")
        print(f"  ISIN     : {isin}")
        print(f"  Name     : {str(name)[:60]}")
        print(f"  CFI      : {cfi_code}")
        if decoded:
            print(f"  Category : {decoded.category} ({decoded.category_code})")
            print(f"  Group    : {decoded.group} ({decoded.group_code})")
            for attr_key, attr_val in decoded.attributes.items():
                label_str = attr_key.replace('_', ' ').title()
                print(f"  {label_str:<20}: {attr_val}")

    # 3. Full decode of the top 5 most common D* (Debt) CFI codes
    print("\n3. Full Decode of Top 5 Debt CFI Codes (D*)")
    print("-" * 60)
    top_debt = firds.data_store.con.execute("""
        SELECT cfi_code, COUNT(*) AS cnt
        FROM instruments
        WHERE cfi_code LIKE 'D%'
          AND LENGTH(cfi_code) = 6
        GROUP BY cfi_code
        ORDER BY cnt DESC
        LIMIT 5
    """).fetchall()

    for cfi_code, cnt in top_debt:
        decoded = decode_cfi(cfi_code)
        print(f"\n  CFI: {cfi_code}  ({cnt:,} instruments)")
        if decoded:
            print(f"  Category : {decoded.category}")
            print(f"  Group    : {decoded.group} ({decoded.group_code})")
            for attr_key, attr_val in decoded.attributes.items():
                label_str = attr_key.replace('_', ' ').title()
                print(f"  {label_str:<20}: {attr_val}")
        else:
            print("  (could not decode)")

    # 4. Direct ISIN lookup with CFI decode
    print("\n4. Direct ISIN Lookup")
    print("-" * 60)
    row = firds.data_store.con.execute(
        "SELECT isin FROM instruments WHERE cfi_code LIKE 'E%' LIMIT 1"
    ).fetchone()
    if row:
        isin = row[0]
        instrument = edm.reference(isin)
        if instrument is not None:
            print(f"  ISIN    : {instrument.get('isin')}")
            print(f"  Name    : {str(instrument.get('full_name', ''))[:60]}")
            print(f"  CFI     : {instrument.get('cfi_code')}")
            cfi_code = instrument.get('cfi_code', '')
            if cfi_code and len(cfi_code) == 6:
                try:
                    decoded = decode_cfi(cfi_code)
                    if decoded:
                        print(f"  Category: {decoded.category}")
                        print(f"  Group   : {decoded.group}")
                except Exception:
                    pass

    # 5. Asset type summary via reference API
    print("\n5. Asset Type Summary")
    print("-" * 60)
    summary = edm.reference.summary()
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
