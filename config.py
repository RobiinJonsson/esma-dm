"""
Global configuration for ESMA Data Manager.

This file contains all configurable settings for database, storage, API access,
and data processing behavior.
"""
from datetime import datetime, timedelta
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent

# Database configuration
DB_BACKEND = 'duckdb'
DB_PATH = PROJECT_ROOT / 'downloads' / 'data' / 'firds' / 'firds.db'
DB_MODE = 'current'  # 'current' (FULINS snapshots) or 'delta' (DLTINS incremental)

# Cache and storage
CACHE_BASE_DIR = PROJECT_ROOT / 'downloads'
CACHE_FIRDS_DIR = CACHE_BASE_DIR / 'data' / 'firds'
DELETE_CSV_AFTER_INDEX = False  # Keep CSV files for testing/validation

# FIRDS API settings
FIRDS_BASE_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
FIRDS_DATE_FROM = "2017-01-01"
FIRDS_DATE_TO = None  # None = today
FIRDS_REQUEST_LIMIT = 10000

# Default date range for queries (last 30 days)
DEFAULT_DAYS_LOOKBACK = 30
DEFAULT_DATE_FROM = (datetime.now() - timedelta(days=DEFAULT_DAYS_LOOKBACK)).strftime('%Y-%m-%d')
DEFAULT_DATE_TO = datetime.now().strftime('%Y-%m-%d')

# Asset types (ISO 10962 CFI Categories)
ASSET_TYPES_ALL = ['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S']
ASSET_TYPE_NAMES = {
    'C': 'Collective Investment Vehicles',
    'D': 'Debt Instruments',
    'E': 'Equities',
    'F': 'Futures',
    'H': 'Non-Standardized Derivatives',
    'I': 'Spot',
    'J': 'Forwards',
    'O': 'Options',
    'R': 'Entitlements',
    'S': 'Swaps'
}

# Asset type to table name mapping (based on ISO 10962 CFI Categories)
ASSET_TABLE_MAP = {
    'C': 'civ',
    'D': 'debt',
    'E': 'equity',
    'F': 'futures',
    'H': 'non_standard',
    'I': 'spot',
    'J': 'forward',
    'O': 'option',
    'R': 'entitlement',
    'S': 'swap'
}

# Update behavior
UPDATE_CHECK_LATEST = True  # Check ESMA registry for newer files
UPDATE_DOWNLOAD_IF_NEWER = True  # Auto-download newer files
UPDATE_DROP_AND_REBUILD = True  # Drop tables and rebuild with new data

# Logging
LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Field coverage validation
MIN_FIELD_COVERAGE_PERCENT = 70.0
FAIL_ON_LOW_COVERAGE = False

# Database schema
MASTER_TABLES = ['instruments', 'listings', 'metadata']
SCHEMA_VERSION = '1.0.0'

# Performance settings
BULK_INSERT_BATCH_SIZE = 10000
MAX_WORKERS = 4  # For parallel downloads
REQUEST_TIMEOUT = 30  # seconds
