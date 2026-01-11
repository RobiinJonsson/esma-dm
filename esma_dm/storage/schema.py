"""
Database schema definitions for FIRDS vectorized storage.

Star schema with master instruments table and asset-specific detail tables.
"""


def create_master_table(con):
    """Create master instruments table with core fields."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            isin VARCHAR PRIMARY KEY,
            cfi_code VARCHAR,
            instrument_type VARCHAR,
            issuer VARCHAR,
            full_name VARCHAR,
            currency VARCHAR,
            source_file VARCHAR,
            indexed_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)


def create_listings_table(con):
    """Create listings table for trading venue information.
    
    One ISIN can have multiple listings on different venues.
    """
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS listings_id_seq START 1
    """)
    
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
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_equity_table(con):
    """Create equity instruments table (E)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS equity_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            dividend_payment_frequency VARCHAR,
            voting_rights_per_share VARCHAR,
            ownership_restriction VARCHAR,
            redemption_type VARCHAR,
            capital_investment_restriction VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_debt_table(con):
    """Create debt instruments table (D)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS debt_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            maturity_date DATE,
            total_issued_nominal_amount DOUBLE,
            nominal_value_per_unit DOUBLE,
            interest_rate_type VARCHAR,
            fixed_rate DOUBLE,
            floating_rate_reference VARCHAR,
            floating_rate_index VARCHAR,
            floating_rate_term_unit VARCHAR,
            floating_rate_term_value VARCHAR,
            floating_rate_basis_spread DOUBLE,
            debt_seniority VARCHAR,
            delivery_type VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_futures_table(con):
    """Create futures instruments table (F)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS futures_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            expiry_date DATE,
            price_multiplier DOUBLE,
            underlying_isin VARCHAR,
            underlying_index_name VARCHAR,
            delivery_type VARCHAR,
            commodity_base_product VARCHAR,
            commodity_sub_product VARCHAR,
            commodity_additional_sub_product VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_option_table(con):
    """Create option instruments table (O, H)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS option_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            expiry_date DATE,
            price_multiplier DOUBLE,
            underlying_isin VARCHAR,
            underlying_index_isin VARCHAR,
            underlying_index_name VARCHAR,
            option_type VARCHAR,
            option_exercise_style VARCHAR,
            strike_price DOUBLE,
            strike_price_percentage DOUBLE,
            strike_price_basis_points DOUBLE,
            strike_price_currency VARCHAR,
            delivery_type VARCHAR,
            fx_type VARCHAR,
            other_notional_currency VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_swap_table(con):
    """Create swap instruments table (S)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS swap_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            expiry_date DATE,
            price_multiplier DOUBLE,
            underlying_isin VARCHAR,
            underlying_lei VARCHAR,
            underlying_index_isin VARCHAR,
            underlying_index_name VARCHAR,
            underlying_index_term_unit VARCHAR,
            underlying_index_term_value VARCHAR,
            underlying_basket_isin VARCHAR,
            delivery_type VARCHAR,
            interest_rate_reference_name VARCHAR,
            interest_rate_term_unit VARCHAR,
            interest_rate_term_value VARCHAR,
            fx_other_notional_currency VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_forward_table(con):
    """Create forward instruments table (J)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS forward_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            expiry_date DATE,
            price_multiplier DOUBLE,
            underlying_isin VARCHAR,
            underlying_index_isin VARCHAR,
            underlying_index_name VARCHAR,
            underlying_basket_isin VARCHAR,
            delivery_type VARCHAR,
            fx_type VARCHAR,
            fx_other_notional_currency VARCHAR,
            commodity_base_product VARCHAR,
            commodity_sub_product VARCHAR,
            commodity_additional_sub_product VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_rights_table(con):
    """Create rights/entitlements table (R)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS rights_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            expiry_date DATE,
            price_multiplier DOUBLE,
            underlying_isin VARCHAR,
            underlying_index_isin VARCHAR,
            underlying_index_name VARCHAR,
            underlying_index_term_unit VARCHAR,
            underlying_index_term_value VARCHAR,
            underlying_basket_isin VARCHAR,
            option_type VARCHAR,
            option_exercise_style VARCHAR,
            strike_price DOUBLE,
            delivery_type VARCHAR,
            commodity_base_product VARCHAR,
            commodity_sub_product VARCHAR,
            commodity_additional_sub_product VARCHAR,
            fx_type VARCHAR,
            fx_other_notional_currency VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_civ_table(con):
    """Create collective investment vehicles table (C)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS civ_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            underlying_isin VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_spot_table(con):
    """Create spot instruments table (I)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS spot_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,
            commodity_base_product VARCHAR,
            commodity_sub_product VARCHAR,
            commodity_additional_sub_product VARCHAR,
            transaction_type VARCHAR,
            final_price_type VARCHAR,
            competent_authority VARCHAR,
            publication_date DATE,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_metadata_table(con):
    """Create metadata table for file tracking."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            file_name VARCHAR PRIMARY KEY,
            indexed_at TIMESTAMP,
            instruments_count INTEGER,
            file_type VARCHAR,
            file_date DATE
        )
    """)


def create_indexes(con):
    """Create indexes for common query patterns."""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_instruments_type ON instruments(instrument_type)",
        "CREATE INDEX IF NOT EXISTS idx_instruments_cfi ON instruments(cfi_code)",
        "CREATE INDEX IF NOT EXISTS idx_listings_isin ON listings(isin)",
        "CREATE INDEX IF NOT EXISTS idx_listings_venue ON listings(trading_venue_id)"
    ]
    
    for index_sql in indexes:
        con.execute(index_sql)


def initialize_schema(con):
    """Initialize complete database schema."""
    create_master_table(con)
    create_listings_table(con)
    create_equity_table(con)
    create_debt_table(con)
    create_futures_table(con)
    create_option_table(con)
    create_swap_table(con)
    create_forward_table(con)
    create_rights_table(con)
    create_civ_table(con)
    create_spot_table(con)
    create_metadata_table(con)
    create_indexes(con)
