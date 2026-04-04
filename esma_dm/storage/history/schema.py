"""
History-specific schema extensions for FIRDS historical database (esma_hist.duckdb).

Extends the standard FIRDS schema with tables for tracking processed files
and baseline FULINS loads required for incremental delta management.
"""

from esma_dm.storage.schema.firds_schema import initialize_schema


def create_baseline_info_table(con):
    """Track which FULINS files have been loaded as baseline."""
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_baseline_info_id START 1")
    con.execute("""
        CREATE TABLE IF NOT EXISTS baseline_info (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_baseline_info_id'),
            file_name VARCHAR UNIQUE NOT NULL,
            file_date DATE NOT NULL,
            asset_type VARCHAR NOT NULL,
            loaded_at TIMESTAMP DEFAULT now(),
            records_loaded INTEGER DEFAULT 0
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_baseline_asset ON baseline_info(asset_type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_baseline_date ON baseline_info(file_date)")


def create_processing_log_table(con):
    """Track which DLTINS files have been applied, with per-type record counts."""
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_processing_log_id START 1")
    con.execute("""
        CREATE TABLE IF NOT EXISTS processing_log (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_processing_log_id'),
            file_name VARCHAR UNIQUE NOT NULL,
            file_date DATE NOT NULL,
            applied_at TIMESTAMP DEFAULT now(),
            records_new INTEGER DEFAULT 0,
            records_modified INTEGER DEFAULT 0,
            records_terminated INTEGER DEFAULT 0,
            records_cancelled INTEGER DEFAULT 0,
            records_skipped INTEGER DEFAULT 0,
            records_error INTEGER DEFAULT 0
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_plog_file_date ON processing_log(file_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_plog_applied_at ON processing_log(applied_at)")


def _recreate_listings_without_fk(con):
    """
    Recreate the listings table without a FK constraint on isin.

    DuckDB 1.4.x incorrectly fires FK constraint violations when doing bulk
    UPDATE operations on the parent (instruments) table even when only non-PK
    columns are changed.  Removing the advisory FK from listings prevents this
    while application-level logic in HistoryStore still enforces consistency.
    """
    # Drop and recreate without FOREIGN KEY (isin) REFERENCES instruments(isin)
    con.execute("DROP TABLE IF EXISTS listings")
    con.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY DEFAULT nextval('listings_id_seq'),
            isin VARCHAR NOT NULL,
            trading_venue_id VARCHAR,
            first_trade_date DATE,
            termination_date DATE,
            admission_approval_date TIMESTAMP,
            request_for_admission_date TIMESTAMP,
            issuer_request VARCHAR,
            source_file VARCHAR,
            indexed_at TIMESTAMP,
            UNIQUE (isin, trading_venue_id)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_listings_isin ON listings(isin)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_listings_venue ON listings(trading_venue_id)")


def initialize_history_schema(con):
    """Initialize complete schema for the esma_hist DuckDB database."""
    initialize_schema(con)
    # Replace listings with FK-free version to avoid DuckDB 1.4.x bulk-update bug
    _recreate_listings_without_fk(con)
    create_baseline_info_table(con)
    create_processing_log_table(con)
