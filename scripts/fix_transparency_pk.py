"""
Rebuild transparency table with a surrogate auto-increment PRIMARY KEY.

Problem: neither isin nor tech_record_id is unique across the full dataset.
- Multiple records per ISIN exist (different methodologies: SINT/YEAR/ESTM/FFWK).
- tech_record_id is only unique within a single source file (resets per file).

Solution: surrogate id BIGINT DEFAULT nextval('seq_transparency_id'), with
isin and tech_record_id as plain indexed columns, and plain INSERT (no OR REPLACE).
"""
import duckdb
import logging

logging.disable(logging.CRITICAL)  # suppress all logging during this script

DB_PATH = r"esma_dm\storage\duckdb\database\esma_current.duckdb"
con = duckdb.connect(DB_PATH)

print("Checking current schema...")
cols = con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name='transparency' ORDER BY ordinal_position"
).fetchall()
for c in cols:
    print(f"  {c[0]:40s} {c[1]}")

print("\nAttempting to alter PRIMARY KEY in place...")
try:
    con.execute("ALTER TABLE transparency DROP PRIMARY KEY")
    con.execute("ALTER TABLE transparency ADD PRIMARY KEY (tech_record_id)")
    print("  SUCCESS: PK changed to tech_record_id in place")
    altered = True
except Exception as e:
    print(f"  ALTER not supported: {e}")
    print("  Will recreate table via DROP + CREATE")
    altered = False

if not altered:
    print("\nRecreating transparency table...")
    # Drop child tables first (FK constraints block dropping the parent)
    con.execute("DROP TABLE IF EXISTS equity_transparency")
    con.execute("DROP TABLE IF EXISTS non_equity_transparency")
    con.execute("DROP TABLE IF EXISTS transparency")

    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_transparency_id START 1")
    con.execute("""
        CREATE TABLE transparency (
            id BIGINT PRIMARY KEY DEFAULT nextval('seq_transparency_id'),
            tech_record_id INTEGER,
            isin TEXT,
            instrument_classification TEXT,
            instrument_type TEXT,
            reporting_period_from DATE,
            reporting_period_to DATE,
            application_period_from DATE,
            application_period_to DATE,
            methodology TEXT,
            total_number_transactions DOUBLE,
            total_volume_transactions DOUBLE,
            liquid_market BOOLEAN,
            average_daily_turnover DOUBLE,
            average_transaction_value DOUBLE,
            standard_market_size DOUBLE,
            average_daily_number_of_trades DOUBLE,
            most_relevant_market_id TEXT,
            most_relevant_market_avg_daily_trades DOUBLE,
            pre_trade_lis_threshold DOUBLE,
            post_trade_lis_threshold DOUBLE,
            pre_trade_ssti_threshold DOUBLE,
            post_trade_ssti_threshold DOUBLE,
            large_in_scale DOUBLE,
            additional_id TEXT,
            additional_avg_daily_trades DOUBLE,
            statistics TEXT,
            file_type TEXT,
            file_date DATE,
            processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_isin ON transparency(isin)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_classification ON transparency(instrument_classification)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_type ON transparency(instrument_type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_liquid ON transparency(liquid_market)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_methodology ON transparency(methodology)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_file_date ON transparency(file_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_file_type ON transparency(file_type)")
    print("  Table recreated with surrogate id PRIMARY KEY (nextval sequence)")

    # Recreate child tables without FK constraints (isin is no longer unique in transparency)
    con.execute("""
        CREATE TABLE equity_transparency (
            isin TEXT PRIMARY KEY,
            attributes JSON
        )
    """)
    con.execute("""
        CREATE TABLE non_equity_transparency (
            isin TEXT PRIMARY KEY,
            attributes JSON
        )
    """)
    print("  Child tables recreated (no FK constraints)")

con.close()
print("\nDone. Now re-run: examples\\11_reindex_transparency.py")
