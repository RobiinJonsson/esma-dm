"""
Display actual DuckDB database table schemas.

Shows the current database structure after the listings table refactoring,
including all tables, columns, types, and relationships.
"""
import duckdb
from pathlib import Path


def get_table_schema(con, table_name: str):
    """Get schema for a specific table."""
    result = con.execute(f"DESCRIBE {table_name}").fetchall()
    return [(row[0], row[1], row[2], row[3]) for row in result]


def print_table_schema(con, table_name: str):
    """Print schema for a table in formatted way."""
    schema = get_table_schema(con, table_name)
    
    print(f"\n{'='*80}")
    print(f"TABLE: {table_name}")
    print('='*80)
    
    # Get row count
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Rows: {count:,}")
    except:
        print("Rows: N/A")
    
    print(f"\n{'Column':<40} {'Type':<20} {'Null':<8} {'Key'}")
    print('-'*80)
    
    for col_name, col_type, null_val, key in schema:
        null_str = "YES" if null_val == "YES" else "NO"
        key_str = key if key else ""
        print(f"{col_name:<40} {col_type:<20} {null_str:<8} {key_str}")


def main():
    """Main entry point."""
    db_path = Path("downloads/data/firds/firds.db")
    
    if not db_path.exists():
        print(f"ERROR: Database not found at {db_path}")
        print("Please run firds.index_cached_files() first to create the database.")
        return
    
    con = duckdb.connect(str(db_path), read_only=True)
    
    print("="*80)
    print("ESMA FIRDS DATABASE SCHEMA")
    print("="*80)
    print(f"\nDatabase: {db_path}")
    print(f"After listings table refactoring (2026-01-05)")
    
    # Get all tables
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' 
        ORDER BY table_name
    """).fetchall()
    
    table_names = [t[0] for t in tables]
    
    print(f"\nTotal tables: {len(table_names)}")
    print(f"Tables: {', '.join(table_names)}")
    
    # Master tables
    print("\n\n" + "="*80)
    print("MASTER TABLES")
    print("="*80)
    print("\nThese tables store core instrument data and relationships")
    
    if 'instruments' in table_names:
        print_table_schema(con, 'instruments')
        print("\nDescription: Master table with one row per ISIN")
        print("Primary Key: isin")
        print("Purpose: Core instrument identification and classification")
    
    if 'listings' in table_names:
        print_table_schema(con, 'listings')
        print("\nDescription: Trading venue listings (one-to-many with instruments)")
        print("Primary Key: id (auto-increment)")
        print("Foreign Key: isin -> instruments(isin)")
        print("Purpose: Store multiple venue listings per instrument")
        print("Note: Replaces trading_venue_id fields in asset tables")
    
    # Asset-specific tables
    print("\n\n" + "="*80)
    print("ASSET-SPECIFIC TABLES")
    print("="*80)
    print("\nThese tables store attributes specific to each asset type")
    
    asset_tables = [
        ('equity_instruments', 'E', 'Equities (shares, stocks, units)'),
        ('debt_instruments', 'D', 'Debt instruments (bonds, notes, bills)'),
        ('futures_instruments', 'F', 'Futures contracts'),
        ('option_instruments', 'O', 'Options and warrants'),
        ('swap_instruments', 'S', 'Swap contracts'),
        ('forward_instruments', 'J', 'Forward contracts'),
        ('rights_instruments', 'R', 'Rights and entitlements'),
        ('civ_instruments', 'C', 'Collective investment vehicles'),
        ('spot_instruments', 'I', 'Spot transactions'),
    ]
    
    for table_name, cfi, description in asset_tables:
        if table_name in table_names:
            print_table_schema(con, table_name)
            print(f"\nAsset Type: {cfi} - {description}")
            print("Primary Key: isin")
            print("Foreign Key: isin -> instruments(isin)")
            print("Note: Listing fields removed (now in listings table)")
    
    # Metadata table
    print("\n\n" + "="*80)
    print("METADATA TABLE")
    print("="*80)
    
    if 'metadata' in table_names:
        print_table_schema(con, 'metadata')
        print("\nDescription: Tracks indexed files and processing history")
        print("Primary Key: file_name")
    
    # Show relationships
    print("\n\n" + "="*80)
    print("TABLE RELATIONSHIPS")
    print("="*80)
    print("""
    instruments (master)
    |-- 1:1 -> equity_instruments
    |-- 1:1 -> debt_instruments
    |-- 1:1 -> futures_instruments
    |-- 1:1 -> option_instruments
    |-- 1:1 -> swap_instruments
    |-- 1:1 -> forward_instruments
    |-- 1:1 -> rights_instruments
    |-- 1:1 -> civ_instruments
    |-- 1:1 -> spot_instruments
    +-- 1:N -> listings (NEW: one instrument can have many venue listings)
    """)
    
    # Show key differences after refactoring
    print("\n" + "="*80)
    print("SCHEMA CHANGES (Post-Refactoring)")
    print("="*80)
    print("""
BEFORE (listing fields in asset tables):
    equity_instruments:
        - trading_venue_id VARCHAR
        - first_trade_date DATE
        - termination_date DATE
        - issuer_request VARCHAR
        ... same in all 9 asset tables ...
    
AFTER (normalized listings table):
    listings (NEW TABLE):
        - id INTEGER (auto-increment)
        - isin VARCHAR (FK)
        - trading_venue_id VARCHAR
        - first_trade_date DATE
        - termination_date DATE
        - admission_approval_date DATE
        - request_for_admission_date DATE
        - issuer_request VARCHAR
        - source_file VARCHAR
        - indexed_at TIMESTAMP
    
    asset tables (CLEANED):
        - Listing fields removed
        - Only asset-specific attributes remain
        - competent_authority VARCHAR (technical metadata)
        - publication_date DATE (technical metadata)

BENEFITS:
    ✓ Normalized design (3NF)
    ✓ No data duplication
    ✓ Proper one-to-many relationships
    ✓ Handles multiple venue listings per ISIN
    ✓ Cleaner asset-specific table schemas
    """)
    
    # Summary statistics
    print("\n" + "="*80)
    print("DATABASE STATISTICS")
    print("="*80)
    
    stats = []
    for table_name in table_names:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            stats.append((table_name, count))
        except:
            stats.append((table_name, 0))
    
    stats.sort(key=lambda x: x[1], reverse=True)
    
    print(f"\n{'Table':<30} {'Rows':>15}")
    print('-'*45)
    for table_name, count in stats:
        print(f"{table_name:<30} {count:>15,}")
    
    total_rows = sum(s[1] for s in stats)
    print('-'*45)
    print(f"{'TOTAL':<30} {total_rows:>15,}")
    
    con.close()


if __name__ == '__main__':
    main()
