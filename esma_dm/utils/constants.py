"""
ESMA register URL constants and endpoints.

This module centralizes all URL constants used for accessing ESMA registers.
All URLs point to the official ESMA registers at registers.esma.europa.eu.

Reference: https://registers.esma.europa.eu/
"""

# Base ESMA domain
ESMA_BASE_URL = "https://registers.esma.europa.eu"

# FIRDS (Financial Instruments Reference Data System) URLs
FIRDS_SOLR_URL = f"{ESMA_BASE_URL}/solr/esma_registers_firds_files/select"
FIRDS_SOLR_ENDPOINT = "solr/esma_registers_firds_files/select"

# FITRS (Financial Instruments Transparency System) URLs
FITRS_SOLR_URL = f"{ESMA_BASE_URL}/solr/esma_registers_fitrs_files/select"
FITRS_SOLR_ENDPOINT = "solr/esma_registers_fitrs_files/select"

# FITRS DVCAP (Deferred Volumes Caps) URLs
DVCAP_SOLR_URL = f"{ESMA_BASE_URL}/solr/esma_registers_dvcap_files/select"
DVCAP_SOLR_ENDPOINT = "solr/esma_registers_dvcap_files/select"

# SSR (Short Selling Regulation) URLs
SSR_SOLR_URL = f"{ESMA_BASE_URL}/solr/esma_registers_mifid_shsexs/select"
SSR_SOLR_ENDPOINT = "solr/esma_registers_mifid_shsexs/select"

# Benchmarks URLs
BENCHMARKS_SOLR_URL = f"{ESMA_BASE_URL}/solr/esma_registers_bmrauth/select"
BENCHMARKS_SOLR_ENDPOINT = "solr/esma_registers_bmrauth/select"

# Default query parameters
DEFAULT_SOLR_PARAMS = {
    'wt': 'xml',
    'indent': 'true',
    'start': 0
}

# File type patterns for filtering
FILE_TYPE_PATTERNS = {
    'FIRDS': {
        'FULL': 'FULINS',
        'DELTA': 'DLTINS',
        'CANCEL': 'FULCAN'
    },
    'FITRS': {
        'EQUITY_FULL': 'FULECR',
        'EQUITY_DELTA': 'DLTECR',
        'NON_EQUITY_FULL': 'FULNCR',
        'NON_EQUITY_DELTA': 'DLTNCR',
        'NON_EQUITY_SUBCLASS_YEARLY': 'FULNCR_NYAR',
        'NON_EQUITY_SUBCLASS_SI': 'FULNCR_SISC'
    }
}

# Asset type codes (CFI first character per ISO 10962)
ASSET_TYPE_CODES = {
    'C': 'Collective Investment Vehicles',
    'D': 'Debt Instruments',
    'E': 'Equities',
    'F': 'Futures',
    'H': 'Rights & Warrants',
    'I': 'Options',
    'J': 'Strategies & Multi-leg',
    'O': 'Others',
    'R': 'Referential Instruments',
    'S': 'Swaps'
}

# Database mode constants
DATABASE_MODES = {
    'CURRENT': 'current',
    'HISTORY': 'history'
}

# Default date ranges
DEFAULT_DATE_FROM = "2018-01-01"  # ESMA FIRDS data available from 2018

# Request limits
DEFAULT_REQUEST_LIMIT = 100
MAX_REQUEST_LIMIT = 1000

# File naming patterns (regex patterns for parsing filenames)
FIRDS_FILENAME_PATTERN = r'(FULINS|DLTINS|FULCAN)_([A-Z])_(\d{8})_(\d+)of(\d+)'
FITRS_FILENAME_PATTERN = r'(FULECR|DLTECR|FULNCR|DLTNCR|FULNCR_NYAR|FULNCR_SISC)_(\d{8})_(\d+)of(\d+)'

# HTTP request settings
HTTP_TIMEOUT = 30  # seconds
HTTP_MAX_RETRIES = 3
HTTP_RETRY_DELAY = 2  # seconds

# Cache settings
CACHE_ENABLED_DEFAULT = True
CACHE_TTL_HOURS = 24  # Time to live for cached files
