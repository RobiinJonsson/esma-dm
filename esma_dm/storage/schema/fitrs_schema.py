"""
FITRS database schema for transparency data.

Stores equity (FULECR) and non-equity (FULNCR) transparency metrics.
"""

def create_transparency_table(con) -> None:
    """
    Create main transparency table for all instrument types.
    
    Full MiFIR transparency fields per ESMA65-8-5240 documentation.
    Supports both FULECR (equity) and FULNCR (non-equity) ISIN-level results.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS transparency (
            tech_record_id INTEGER,
            isin TEXT PRIMARY KEY,
            
            -- Classification
            instrument_classification TEXT,
            instrument_type TEXT,
            
            -- Reporting Period (historical data used for calculation)
            reporting_period_from DATE,
            reporting_period_to DATE,
            
            -- Application Period (when results are valid for regulatory use)
            application_period_from DATE,
            application_period_to DATE,
            
            -- Methodology (SINT=SI historical, YEAR=yearly, ESTM=estimation, FFWK=framework)
            methodology TEXT,
            
            -- Transaction Metrics (for SINT methodology only)
            total_number_transactions DOUBLE,
            total_volume_transactions DOUBLE,
            
            -- Liquidity Assessment
            liquid_market BOOLEAN,
            
            -- Equity Transparency Metrics (FULECR)
            average_daily_turnover DOUBLE,
            average_transaction_value DOUBLE,
            standard_market_size DOUBLE,
            average_daily_number_of_trades DOUBLE,
            
            -- Most Relevant Market (MiFIR Art.4(1)(a))
            most_relevant_market_id TEXT,
            most_relevant_market_avg_daily_trades DOUBLE,
            
            -- Non-Equity Transparency Thresholds (FULNCR)
            pre_trade_lis_threshold DOUBLE,
            post_trade_lis_threshold DOUBLE,
            pre_trade_ssti_threshold DOUBLE,
            post_trade_ssti_threshold DOUBLE,
            
            -- Large In Scale (may be used differently for equity vs non-equity)
            large_in_scale DOUBLE,
            
            -- Additional fields from XML
            additional_id TEXT,
            additional_avg_daily_trades DOUBLE,
            statistics TEXT,
            
            -- File tracking
            file_type TEXT,
            file_date DATE,
            processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_classification ON transparency(instrument_classification)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_type ON transparency(instrument_type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_liquid ON transparency(liquid_market)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_methodology ON transparency(methodology)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_file_date ON transparency(file_date)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_transparency_file_type ON transparency(file_type)")


def create_equity_transparency_details_table(con) -> None:
    """
    Create detailed equity transparency table (FULECR specific fields).
    FULECR files have same structure as FULNCR - reserved for future extensions.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS equity_transparency (
            isin TEXT PRIMARY KEY,
            
            -- Reserved for FULECR-specific fields
            attributes JSON,
            
            -- Reference
            FOREIGN KEY (isin) REFERENCES transparency(isin)
        )
    """)


def create_non_equity_transparency_details_table(con) -> None:
    """
    Create detailed non-equity transparency table (FULNCR specific fields).
    FULNCR files have same structure as FULECR - reserved for future extensions.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS non_equity_transparency (
            isin TEXT PRIMARY KEY,
            
            -- Reserved for FULNCR-specific fields  
            attributes JSON,
            
            -- Reference
            FOREIGN KEY (isin) REFERENCES transparency(isin)
        )
    """)


def create_subclass_transparency_table(con) -> None:
    """
    Create table for non-equity sub-class level transparency results.

    Stores FULNCR_NYAR (yearly) and FULNCR_SISC (SI historical) files.
    Per ESMA65-8-5240 section 2.3, paragraphs 8-10.
    """
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_subclass_transparency_id START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS subclass_transparency (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_subclass_transparency_id'),
            tech_record_id INTEGER,

            -- Asset/Sub-asset Class Identification
            asset_class TEXT,
            sub_asset_class_code TEXT,
            sub_asset_class_description TEXT,

            -- Segmentation Criteria (30+ possible criteria, stored as JSON)
            segmentation_criteria JSON,

            -- Calculation Type
            calculation_type TEXT,
            methodology TEXT,

            -- Reporting Period
            reporting_period_from DATE,
            reporting_period_to DATE,

            -- Application Period
            application_period_from DATE,
            application_period_to DATE,

            -- Liquidity (for NYAR yearly calculations only)
            liquid_market BOOLEAN,

            -- Transaction Metrics (for SISC SI calculations)
            total_number_transactions DOUBLE,
            total_volume_transactions DOUBLE,

            -- Average Daily Turnover (for NYAR yearly calculations)
            average_daily_turnover DOUBLE,

            -- Thresholds (for NYAR yearly calculations)
            pre_trade_lis_threshold DOUBLE,
            post_trade_lis_threshold DOUBLE,
            pre_trade_ssti_threshold DOUBLE,
            post_trade_ssti_threshold DOUBLE,

            -- File tracking
            file_name TEXT,
            file_type TEXT,
            file_date DATE,
            processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_subclass_asset ON subclass_transparency(asset_class)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_subclass_code ON subclass_transparency(sub_asset_class_code)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_subclass_calc_type ON subclass_transparency(calculation_type)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_subclass_file_date ON subclass_transparency(file_date)")


def create_transparency_metadata_table(con) -> None:
    """
    Create metadata table for tracking FITRS file processing.
    """
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_transparency_metadata_id START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS transparency_metadata (
            id INTEGER PRIMARY KEY DEFAULT nextval('seq_transparency_metadata_id'),
            file_name TEXT NOT NULL,
            file_type TEXT,
            instrument_type TEXT,
            publication_date DATE,
            processed_date TIMESTAMP,
            record_count INTEGER
        )
    """)
    
    # Create indexes separately
    con.execute("CREATE INDEX IF NOT EXISTS idx_metadata_file ON transparency_metadata(file_name)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_metadata_pub_date ON transparency_metadata(publication_date)")


def initialize_fitrs_schema(con) -> None:
    """
    Initialize complete FITRS database schema.
    
    Args:
        con: DuckDB connection
    """
    create_transparency_table(con)
    create_equity_transparency_details_table(con)
    create_non_equity_transparency_details_table(con)
    create_subclass_transparency_table(con)
    create_transparency_metadata_table(con)


def get_fitrs_schema_info() -> dict:
    """
    Get schema information for FITRS tables.
    
    Returns:
        Dictionary with table names and expected columns
    """
    return {
        'transparency': {
            'description': 'ISIN-level transparency metrics (FULECR/FULNCR/DLTECR/DLTNCR)',
            'columns': [
                'tech_record_id', 'isin', 'instrument_classification', 'instrument_type',
                'reporting_period_from', 'reporting_period_to',
                'application_period_from', 'application_period_to',
                'methodology', 'total_number_transactions', 'total_volume_transactions',
                'liquid_market', 'average_daily_turnover', 'average_transaction_value',
                'standard_market_size', 'average_daily_number_of_trades',
                'most_relevant_market_id', 'most_relevant_market_avg_daily_trades',
                'pre_trade_lis_threshold', 'post_trade_lis_threshold',
                'pre_trade_ssti_threshold', 'post_trade_ssti_threshold',
                'large_in_scale', 'additional_id', 'additional_avg_daily_trades',
                'statistics', 'file_type', 'file_date', 'processed_date'
            ]
        },
        'subclass_transparency': {
            'description': 'Sub-class level transparency metrics (FULNCR_NYAR/FULNCR_SISC)',
            'columns': [
                'id', 'tech_record_id', 'asset_class', 'sub_asset_class_code',
                'sub_asset_class_description', 'segmentation_criteria',
                'calculation_type', 'methodology',
                'reporting_period_from', 'reporting_period_to',
                'application_period_from', 'application_period_to',
                'liquid_market', 'total_number_transactions', 'total_volume_transactions',
                'average_daily_turnover',
                'pre_trade_lis_threshold', 'post_trade_lis_threshold',
                'pre_trade_ssti_threshold', 'post_trade_ssti_threshold',
                'file_name', 'file_type', 'file_date', 'processed_date'
            ]
        },
        'equity_transparency': {
            'description': 'Equity ISIN references (reserved for FULECR-specific extensions)',
            'columns': ['isin', 'attributes']
        },
        'non_equity_transparency': {
            'description': 'Non-equity ISIN references (reserved for FULNCR-specific extensions)',
            'columns': ['isin', 'attributes']
        },
        'transparency_metadata': {
            'description': 'FITRS file processing metadata',
            'columns': [
                'id', 'file_name', 'file_type', 'instrument_type',
                'publication_date', 'processed_date', 'record_count'
            ]
        }
    }
