"""
Example: Analytics Dashboard

Demonstrates the analytics capabilities built into FIRDSClient: per-asset-type
statistics, database health checks, and the print_analytics_summary() method.
"""

import esma_dm as edm


def main():
    print("ESMA Data Manager - Analytics Dashboard")
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

    # 1. Quick analytics summary printed to console
    print("\n1. Analytics Summary")
    print("-" * 60)
    firds.print_analytics_summary()

    # 2. Full analytics dashboard — scalar fields only
    print("\n2. Analytics Dashboard Details")
    print("-" * 60)
    dashboard = firds.get_analytics_dashboard()
    scalar_keys = [
        'total_instruments', 'total_listings', 'unique_venues',
        'cross_listed_count', 'avg_listings_per_instrument',
        'mode', 'last_updated',
    ]
    for key in scalar_keys:
        val = dashboard.get(key)
        if val is not None:
            print(f"  {key:<35}: {val}")

    # 3. Per-asset-type breakdown
    print("\n3. Per-Asset-Type Breakdown")
    print("-" * 60)
    breakdown = firds.get_asset_breakdown()
    print(breakdown.to_string(index=False))

    # 4. Database statistics (scalar fields only)
    print("\n4. Database Statistics")
    print("-" * 60)
    stats = firds.get_database_stats()
    for key, val in stats.items():
        if val is not None and not isinstance(val, (dict, list)):
            print(f"  {key:<30}: {val}")

    # 5. Available subtype output models
    print("\n5. Available Subtype Output Models")
    print("-" * 60)
    try:
        subtypes = edm.reference.subtypes()
        print(subtypes.to_string(index=False))
    except Exception as e:
        print(f"  subtypes() not available: {e}")


if __name__ == "__main__":
    main()
