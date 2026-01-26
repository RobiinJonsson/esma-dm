"""
Database schema definitions for FIRDS vectorized storage.

Star schema with master instruments table and asset-specific detail tables.
"""


def create_master_table(con):
    """Create master instruments table with core fields and historical tracking."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            isin VARCHAR PRIMARY KEY,
            cfi_code VARCHAR,
            instrument_type VARCHAR,
            issuer VARCHAR,
            full_name VARCHAR,
            short_name VARCHAR,
            currency VARCHAR,
            
            -- Technical attributes (ESMA RTS 23)
            competent_authority VARCHAR,
            publication_date DATE,
            
            -- Historical tracking (ESMA Section 8)
            valid_from_date DATE,
            valid_to_date DATE,
            latest_record_flag BOOLEAN DEFAULT TRUE,
            record_type VARCHAR,
            version_number INTEGER DEFAULT 1,
            
            -- Source tracking
            source_file VARCHAR,
            source_file_type VARCHAR,
            last_update_timestamp TIMESTAMP,
            inconsistency_indicator VARCHAR,
            
            -- System fields
            indexed_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    
    # Create indexes for historical queries
    con.execute("CREATE INDEX IF NOT EXISTS idx_instruments_latest ON instruments(latest_record_flag)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_instruments_valid_from ON instruments(valid_from_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_instruments_valid_to ON instruments(valid_to_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_instruments_record_type ON instruments(record_type)")


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
    """Create equity instruments table (E) based on actual FIRDS data structure."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS equity_instruments (
            isin VARCHAR PRIMARY KEY,
            underlying_instrument VARCHAR,  -- RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN
            commodity_derivative_indicator BOOLEAN,  -- RefData_FinInstrmGnlAttrbts_CmmdtyDerivInd
            version_number INTEGER DEFAULT 1,
            FOREIGN KEY (isin) REFERENCES instruments(isin)
        )
    """)


def create_debt_table(con):
    """Create debt instruments table (D) based on actual FIRDS data structure."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS debt_instruments (
            isin VARCHAR PRIMARY KEY,
            total_issued_nominal_amount DOUBLE,  -- RefData_DebtInstrmAttrbts_TtlIssdNmnlAmt
            maturity_date DATE,                  -- RefData_DebtInstrmAttrbts_MtrtyDt
            nominal_value_per_unit DOUBLE,       -- RefData_DebtInstrmAttrbts_NmnlValPerUnit
            fixed_interest_rate DOUBLE,          -- RefData_DebtInstrmAttrbts_IntrstRate_Fxd
            debt_seniority VARCHAR,              -- RefData_DebtInstrmAttrbts_DebtSnrty
            version_number INTEGER DEFAULT 1,
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
    """Create option instruments table (O) using FIRDS data mapping specification."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS option_instruments (
            isin VARCHAR PRIMARY KEY,
            short_name VARCHAR,                           -- RefData_FinInstrmGnlAttrbts_ShrtNm
            expiry_date DATE,                            -- RefData_DerivInstrmAttrbts_XpryDt
            price_multiplier DECIMAL,                    -- RefData_DerivInstrmAttrbts_PricMltplr
            underlying_isin VARCHAR,                     -- RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_ISIN
            underlying_index_isin VARCHAR,               -- RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_ISIN
            underlying_index_name VARCHAR,               -- RefData_DerivInstrmAttrbts_UndrlygInstrm_Sngl_Indx_Nm_RefRate_Nm
            option_type VARCHAR,                         -- RefData_DerivInstrmAttrbts_OptnTp
            option_exercise_style VARCHAR,               -- RefData_DerivInstrmAttrbts_OptnExrcStyle
            strike_price DECIMAL,                        -- RefData_DerivInstrmAttrbts_StrkPric_Pric_MntryVal_Amt
            strike_price_percentage DECIMAL,             -- RefData_DerivInstrmAttrbts_StrkPric_Pric_Pctg
            strike_price_basis_points DECIMAL,           -- RefData_DerivInstrmAttrbts_StrkPric_Pric_BsisPts
            strike_price_currency VARCHAR,               -- Not available in current data
            delivery_type VARCHAR,                       -- RefData_DerivInstrmAttrbts_DlvryTp
            fx_type VARCHAR,                            -- RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_FxTp
            other_notional_currency VARCHAR,            -- RefData_DerivInstrmAttrbts_AsstClssSpcfcAttrbts_FX_OthrNtnlCcy
            competent_authority VARCHAR,                 -- RefData_TechAttrbts_RlvntCmptntAuthrty
            publication_date DATE,                       -- RefData_TechAttrbts_PblctnPrd_FrDt
            version_number INTEGER DEFAULT 1
        )
    """)


def create_swap_table(con):
    """Create swap instruments table (S) based on actual FIRDS data structure."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS swap_instruments (
            isin VARCHAR PRIMARY KEY,
            price_multiplier DOUBLE,             -- RefData_DerivInstrmAttrbts_PricMltplr
            delivery_type VARCHAR,               -- RefData_DerivInstrmAttrbts_DlvryTp  
            expiry_date DATE,                    -- RefData_DerivInstrmAttrbts_XpryDt
            asset_class_specific VARCHAR,        -- RefData_DerivInstrmAttrbts_AsstClssSpcfc*
            underlying_instrument VARCHAR,       -- RefData_DerivInstrmAttrbts_UndrlygInstrm*
            version_number INTEGER DEFAULT 1,
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
            version_number INTEGER DEFAULT 1,
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
            version_number INTEGER DEFAULT 1,
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
            version_number INTEGER DEFAULT 1,
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
            version_number INTEGER DEFAULT 1,
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


def create_cancellations_table(con):
    """Create table for cancelled records (FULCAN files)."""
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_cancellations_id START 1;
        
        CREATE TABLE IF NOT EXISTS cancellations (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_cancellations_id'),
            isin VARCHAR NOT NULL,
            trading_venue_id VARCHAR,
            cancellation_date DATE,
            cancellation_reason VARCHAR,
            original_publication_date DATE,
            source_file VARCHAR,
            indexed_at TIMESTAMP
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cancellations_isin ON cancellations(isin)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_cancellations_date ON cancellations(cancellation_date)")


def create_instrument_history_table(con):
    """
    Create table for full instrument version history.
    
    Stores all versions of each instrument to support temporal queries
    as per ESMA Section 8 guidance.
    """
    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_instrument_history_id START 1;
        
        CREATE TABLE IF NOT EXISTS instrument_history (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_instrument_history_id'),
            isin VARCHAR NOT NULL,
            version_number INTEGER NOT NULL,
            valid_from_date DATE NOT NULL,
            valid_to_date DATE,
            record_type VARCHAR,
            cfi_code VARCHAR,
            full_name VARCHAR,
            issuer VARCHAR,
            attributes JSON,
            source_file VARCHAR,
            source_file_type VARCHAR,
            indexed_at TIMESTAMP,
            UNIQUE(isin, version_number)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_history_isin ON instrument_history(isin)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_history_valid_from ON instrument_history(valid_from_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_history_valid_to ON instrument_history(valid_to_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_history_version ON instrument_history(isin, version_number)")


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
    """Initialize complete database schema with historical tracking."""
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
    create_cancellations_table(con)
    create_instrument_history_table(con)
    create_indexes(con)
