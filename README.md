# ESMA Data Manager

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) published reference data and transparency information.

**Note**: This is an unofficial package and is not endorsed by ESMA. All data is sourced from publicly available ESMA registers.

## Features

- **FIRDS**: Financial Instruments Reference Data System with complete asset type coverage
- **FITRS**: Financial Instruments Transparency System with equity and non-equity data
- **10 Asset Types**: Full support for C, D, E, F, H, I, J, O, R, S instrument types
- **CFI Classification**: Complete ISO 10962 decoding with full attribute descriptions
- **Instrument Lookup**: `esma-dm firds reference <ISIN>` — master fields, CFI classification, and typed detail columns
- **Instrument Search**: `esma-dm firds search <query>` — search by name or ISIN prefix with asset-type filter
- **Database CLI**: `esma-dm db stats|reinit|drop` — inspect, reset, and manage the local DuckDB database
- **Transparency Loading**: `esma-dm fitrs index` — load 10M+ transparency records from local FITRS cache into the unified database
- **Complete Field Coverage**: Full name, short name, and all ESMA reference data fields
- **High Performance**: Vectorized bulk loading at 33,000+ instruments/second
- **DuckDB Storage**: Fast analytical queries on star schema with 12 normalized tables
- **Cross-Database Queries**: Join FIRDS reference data with FITRS transparency metrics
- **Normalized Design**: Separate listings table for one-to-many venue relationships
- **RTS 23 Compliance**: Full support for regulatory technical standards
- **SQL Interface**: Run complex queries on 2.3M+ instruments
- **Database Management**: Initialize, drop, and update methods for lifecycle control
- **Modular Architecture**: Clean separation of concerns with utility modules and focused components
- **ISO Standard Validation**: Built-in validators for ISIN, LEI, CFI, and MIC codes
- **Transparency API**: Query liquidity and turnover metrics via edm.transparency()
- **Centralized Configuration**: Mode-aware settings with smart defaults and validation
- **Clean Imports**: Simplified import patterns for better IDE support and maintainability

## Architecture

### Modular Design

The package follows a clean, modular architecture with clear separation of concerns:

```
esma_dm/
├── clients/              # Data source clients
│   ├── firds/            # Modular FIRDS client (6 focused modules)
│   ├── fitrs.py          # FITRS transparency client
│   ├── ssr.py            # Short selling regulation client
│   └── benchmarks.py     # Benchmarks client
├── cli/                  # Command-line interface commands
├── config/               # Configuration management
│   ├── base.py           # Base configuration class
│   └── registry.py       # Specialized configurations
├── file_manager/         # File download and caching layer
├── models/               # Data models and mappers
├── storage/              # Database backends
│   ├── duckdb/           # DuckDB implementation (5 focused modules)
│   ├── fitrs/            # FITRS storage backend
│   └── schema/           # Table definitions
├── utils/                # Shared utilities
│   ├── validators.py     # ISO standard validators
│   ├── constants.py      # URLs and configuration
│   ├── query_builder.py  # SQL query patterns
│   └── shared_utils.py   # Common utilities
├── reference_api.py      # Hierarchical reference API
└── transparency_api.py   # Transparency data API
```

### Benefits

- **Maintainability**: Each module has a single responsibility with clean imports
- **Testability**: Components can be tested independently
- **Extensibility**: Easy to add new data sources or storage backends
- **Reusability**: Shared utilities eliminate code duplication
- **Type Safety**: Full type hints and validation
- **Configuration Management**: Centralized settings with mode-specific optimizations
- **Developer Experience**: Clean project structure with simplified import patterns

## Installation

```bash
# Install in development mode
pip install -e .
```

## Quick Start

```python
import esma_dm as edm

# 1. Initialize with default configuration
firds = edm.FIRDSClient()  # Uses smart defaults from configuration
firds.data_store.initialize()

# Or with custom parameters
firds = edm.FIRDSClient(mode='current', date_from='2025-01-01', limit=500)

# 2. Download latest files (uses intelligent caching)
firds.get_latest_full_files(asset_type='E')  # Equities

# 3. Load database from CSV files
results = firds.index_cached_files()
print(f'Indexed {results["total_instruments"]:,} instruments')

# 4. Query reference data
instrument = edm.reference('SE0000242455')
print(instrument['short_name'])  # SWEDBANK/SH A

# 5. Asset type queries
swap_types = edm.reference.swap.types()
print(f'Unique swap CFI codes: {len(swap_types)}')

# 6. Statistics
print(f'Total swaps: {edm.reference.swap.count():,}')
summary = edm.reference.summary()
```

## Command Line Interface

The package provides a comprehensive CLI for managing ESMA data files, databases, and analysis tasks. All commands use rich terminal formatting for improved readability.

### Installation

After installing the package, the CLI is available via:

```bash
# Direct command
esma-dm --help

# Or via Python module
python -m esma_dm --help
```

### File Management Commands

#### List Available Files

List files from ESMA FIRDS register with optional filters:

```bash
# List all available files
esma-dm firds list

# Filter by file type (FULINS = full snapshots, DLTINS = delta updates)
esma-dm firds list --type FULINS

# Filter by asset type (E = Equity, D = Debt, S = Swap, etc.)
esma-dm firds list --asset E

# Combine filters
esma-dm firds list --type FULINS --asset E --limit 50

# Date range filtering
esma-dm firds list --date-from 2026-01-01 --date-to 2026-01-31
```

#### Download Files

Download latest files from ESMA with caching support:

```bash
# Download latest equity FULINS files
esma-dm firds download --type FULINS --asset E

# Download latest delta files
esma-dm firds download --type DLTINS --asset D

# Force fresh download (ignore cache)
esma-dm firds download --asset E --update

# Download files for specific date
esma-dm firds download --type DLTINS --asset E --date 2026-01-15
```

#### List Cached Files

View files already downloaded to local cache:

```bash
# List all cached files
esma-dm firds cache

# Filter by asset type
esma-dm firds cache --asset E

# Filter by file type and asset
esma-dm firds cache --type FULINS --asset D
```

The command displays file name, size, and modification date in a formatted table.

#### Look Up an Instrument by ISIN

Query the local database for a single instrument:

```bash
# Equity lookup — master fields, CFI classification, equity-specific detail
esma-dm firds reference US0378331005

# Swap lookup — shows expiry, underlying, delivery type
esma-dm firds reference EZGR2K9V7V85

# Swedish equity
esma-dm firds reference SE0000242455
```

Output includes three sections:
1. **Master fields** — ISIN, CFI code, full name, short name, issuer LEI, source file, instrument type
2. **CFI Classification** — category, group, and all four ISO 10962 attribute labels with full descriptions
3. **Asset-Specific Fields** — columns from the typed detail table (e.g. `expiry_date`, `strike_price`, `underlying_instrument`, `delivery_type`)

#### Search Instruments by Name or ISIN

Search the local database by instrument name or ISIN prefix:

```bash
# Search by name (case-insensitive)
esma-dm firds search "apple"

# Narrow to a specific asset type
esma-dm firds search "volkswagen" --asset E --limit 10

# Search by ISIN prefix
esma-dm firds search "US0378"

# Search across swaps
esma-dm firds search "equity swap" --asset S --limit 20
```

#### Inspect File Structure

List all field names (columns) in a CSV file:

```bash
# Using filename (searches cache directory)
esma-dm firds fields FULINS_E_20260117_01of02_data.csv

# Using absolute path
esma-dm firds fields C:/path/to/file.csv
```

#### Preview File Contents

Display the first N rows of a file:

```bash
# Show first 10 rows (default)
esma-dm firds head FULINS_E_20260117_01of02_data.csv

# Custom number of rows
esma-dm firds head FULINS_E_20260117_01of02_data.csv --rows 20

# Select specific columns
esma-dm firds head FULINS_E_20260117_01of02_data.csv \
  --columns "Id,RefData_FinInstrmGnlAttrbts_FullNm,RefData_FinInstrmGnlAttrbts_ClssfctnTp"

# Short form
esma-dm firds head FULINS_E_20260117_01of02_data.csv -n 5
```

### CLI Features

- **Rich Formatting**: Beautiful tables with proper alignment and colors
- **Progress Indicators**: Spinners and progress bars for long operations
- **Smart Path Resolution**: Automatically finds files in cache directory
- **Comprehensive Help**: Use `--help` with any command for detailed usage
- **Error Handling**: Clear error messages with actionable suggestions

### Package Information

```bash
# Show package version and info
esma-dm info

# Show version only
esma-dm --version
```

### Database Management

Manage the local DuckDB database from the command line.

```bash
# Show database statistics (instruments, listings, asset type breakdown)
esma-dm db stats

# Include per-table row counts for all 24 tables
esma-dm db stats --tables

# Drop and reinitialize the schema (interactive confirmation)
esma-dm db reinit

# Same for history-mode database
esma-dm db reinit --mode history

# Skip confirmation prompt
esma-dm db reinit --yes

# Drop without reinitializing
esma-dm db drop --yes
```

`db stats` output includes:

- Database file path and size on disk
- Total instruments and listings, distinct trading venues
- Per-asset-type instrument counts with the corresponding detail table name
- FITRS transparency table row counts

`db reinit` is the correct command after a corrupted or duplicate-insert run. It drops the database file and calls schema initialization before prompting to re-index from cache.

### Transparency Data (FITRS)

Download and index ESMA FITRS transparency results into the unified database.

```bash
# List available transparency files from ESMA register
esma-dm fitrs list
esma-dm fitrs list --type FULECR
esma-dm fitrs list --type FULNCR --instrument non-equity

# Download latest full files to local cache (~1.8 GB for all asset types)
esma-dm fitrs download

# List cached transparency files with sizes
esma-dm fitrs cache

# Load all cached CSV files into the transparency tables
esma-dm fitrs index

# Load only equity (FULECR) or non-equity (FULNCR) results
esma-dm fitrs index --type FULECR
esma-dm fitrs index --type FULNCR

# Reinit DB schema and immediately load FITRS cache in one step
esma-dm db reinit --fitrs
```

After a full `fitrs index` run the unified database contains:

| Table | Rows | Description |
|---|---|---|
| `transparency` | 10.7M | ISIN-level results, both equity and non-equity |
| `equity_transparency` | 29K | Equity ISIN index (FULECR coverage) |
| `non_equity_transparency` | 10.7M | Non-equity ISIN index (FULNCR coverage) |

File types supported by `fitrs index`: `FULECR`, `FULNCR`, `DLTECR`, `DLTNCR`.

## Configuration Management

The package now uses centralized configuration with mode-specific optimizations:

```python
from esma_dm.config import get_firds_config, get_database_config

# Get mode-specific configuration
current_config = get_firds_config('current')
print(f'Cache enabled: {current_config.cache_enabled}')  # True
print(f'Default limit: {current_config.default_limit}')  # 100

history_config = get_firds_config('history')
print(f'Cache enabled: {history_config.cache_enabled}')  # False (always fresh)
print(f'Batch size: {history_config.batch_size}')       # 5000 (smaller for accuracy)

# Configuration validation
safe_limit = current_config.validate_limit(999999)  # Returns 1000 (max allowed)
date_range = current_config.get_date_range('2025-01-01')  # Returns validated range
```

## Operation Modes

### Current Mode (Default)

Optimized for querying latest instrument data with minimal storage overhead:

```python
firds = edm.FIRDSClient(mode='current')  # Uses esma_current.duckdb
firds.data_store.initialize()

# Download uses cached files by default (fast during development)
firds.get_latest_full_files(asset_type='E')  # Uses cached files
firds.get_latest_full_files(asset_type='E', update=True)  # Force fresh download

# Latest snapshots only (9 core columns)
results = firds.index_cached_files()
```

**Use current mode when:**
- You only need latest instrument reference data
- Storage efficiency is important (9 core columns vs 17)
- Queries focus on current active instruments

### History Mode

Full ESMA Section 8.2 compliance with version tracking and delta processing:

```python
firds = edm.FIRDSClient(mode='history')  # Uses esma_history.duckdb
firds.data_store.initialize()

# Initial load with FULINS files
firds.get_latest_full_files(asset_type='E')
firds.index_cached_files()

# Process daily DLTINS delta files
stats = firds.process_delta_files(
    asset_type='E',
    date_from='2026-01-04',
    date_to='2026-01-11'
)

print(f"Processed {stats['records_processed']} delta records")
print(f"NEW: {stats['new']}, MODIFIED: {stats['modified']}")
print(f"TERMINATED: {stats['terminated']}, CANCELLED: {stats['cancelled']}")

# Query historical states
history = firds.data_store.get_instrument_version_history('GB00B1YW4409')
active_on_date = firds.data_store.get_instruments_active_on_date('2025-12-31')
```

**Use history mode when:**
- You need full version tracking and audit trails
- Processing DLTINS delta files for regulatory compliance
- Temporal queries required (point-in-time instrument state)
- Maintaining complete lifecycle history per ESMA guidance

## Complete Workflow

### 1. Installation

```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -e .
```

### 2. Initialize Database

Initialize the database before loading any data. This creates the schema and verifies structure:

```python
from esma_dm import FIRDSClient

# Choose mode: 'current' for snapshots or 'history' for version tracking
firds = FIRDSClient(mode='current')  # Default mode

# Initialize database (creates schema and verifies structure)
result = firds.data_store.initialize()
print(f"Status: {result['status']}")
print(f"Database: {firds.data_store.db_path}")
print(f"Tables created: {result['tables_created']}")

# If database already exists, it will verify schema without reinitializing
# To force recreation, drop first:
# firds.data_store.drop(confirm=True)
# firds.data_store.initialize()
```

**Mode selection:**
- `mode='current'`: esma_current.duckdb - Latest snapshots only
- `mode='history'`: esma_history.duckdb - Full version tracking

### 3. Download Data

Download FIRDS files from ESMA registry. Methods use cached files by default for faster development:

```python
# Download latest files for specific asset types (uses cache by default)
firds.get_latest_full_files(asset_type='E')  # Equities from cache
firds.get_latest_full_files(asset_type='D')  # Debt from cache
firds.get_latest_full_files(asset_type='S')  # Swaps from cache

# Force fresh download when needed
firds.get_latest_full_files(asset_type='E', update=True)

# Or download all asset types
for asset_type in ['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S']:
    try:
        firds.get_latest_full_files(asset_type=asset_type)
    except Exception as e:
        print(f"Could not download {asset_type}: {e}")
```

### 4. Load Database

Index downloaded CSV files into the database:

```python
# Index latest files for all asset types (default)
results = firds.index_cached_files()
print(f"Indexed {results['total_instruments']:,} instruments")
print(f"Indexed {results['total_listings']:,} venue listings")
print(f"Files processed: {results['files_processed']}")

# Index only specific asset type
results = firds.index_cached_files(asset_type='E')  # Equities only

# Index all versions of files (not just latest)
results = firds.index_cached_files(latest_only=False)

# Delete CSV files after successful indexing
results = firds.index_cached_files(delete_csv=True)
```

### 5. Query Data

```python
# Simple lookup via reference API
instrument = edm.reference('SE0000242455')

# Get CFI classification details
classification = instrument['cfi_classification']

# Asset type queries
swap_types = edm.reference.swap.types()  # All swap CFI codes
equity_count = edm.reference.equity.count()  # Count equities
samples = edm.reference.forward.sample(10)  # Sample forwards

# Direct SQL queries
df = firds.data_store.query("""
    SELECT instrument_type, COUNT(*) as count
    FROM instruments
    GROUP BY instrument_type
    ORDER BY count DESC
""")
```

## Database Management

### Verify Schema

Verify existing database structure without modifications:

```python
# Verify schema matches data models
result = firds.data_store.initialize(verify_only=True)
print(f"Tables verified: {result['tables_verified']}")
```

### Drop Database

Drop the database completely to start fresh or free up space:

```python
# Drop database (requires explicit confirmation for safety)
result = firds.data_store.drop(confirm=True)
print(f"Status: {result['status']}")
print(f"Deleted: {result['file_size_bytes']:,} bytes")

# Reinitialize fresh database
firds.data_store.initialize(mode='current')
```

**Safety Feature**: The drop method requires `confirm=True` to prevent accidental data loss.

### Drop and Rebuild

Complete workflow to drop and rebuild with fresh data:

```python
# 1. Drop existing database
firds.data_store.drop(confirm=True)

# 2. Reinitialize schema
firds.data_store.initialize()

# 3. Download latest data (force fresh download)
for asset_type in ['E', 'D', 'S']:  # Example: equities, debt, swaps
    firds.get_latest_full_files(asset_type=asset_type, update=True)

# 4. Reload database
results = firds.index_cached_files()
print(f"Indexed {results['total_instruments']:,} instruments")
```

### Update Data

Update with newer FIRDS files:

```python
# Update specific asset type
result = firds.data_store.update(asset_type='E')

# Full rebuild (all asset types)
result = firds.data_store.update()
```

## Historical Database (ESMA Section 8 Compliance)

The database maintains complete version history per ESMA65-8-5014 Section 8 requirements:

### Temporal Tracking

Every instrument record includes:
- `valid_from_date`: When this version became effective
- `valid_to_date`: When this version was superseded (NULL for latest)
- `latest_record_flag`: TRUE for current version
- `record_type`: NEW, MODIFIED, TERMINATED, CANCELLED
- `version_number`: Sequential version identifier
- `source_file_type`: FULINS (full), DLTINS (delta), or FULCAN (cancellation)

### Query Historical State

```python
# Get current/latest versions only
current = firds.data_store.get_latest_instruments(limit=1000)

# Get instruments active on a specific date
active = firds.data_store.get_instruments_active_on_date('2024-06-15')

# Get instrument state as it was on a specific date
historical = firds.data_store.get_instrument_state_on_date('GB00B1YW4409', '2023-06-15')

# Get complete version history for an ISIN
versions = firds.data_store.get_instrument_version_history('GB00B1YW4409')
print(f"Versions: {len(versions)}")

# Track modifications since a date
modified = firds.data_store.get_modified_instruments_since('2025-01-01')

# Query cancelled instruments (FULCAN files)
cancelled = firds.data_store.get_cancelled_instruments(since_date='2024-01-01')
```

### Version History Table

The `instrument_history` table stores all versions with:
- Full instrument attributes in JSON format
- Temporal validity dates
- Record type from delta XML (<NewRcrd>, <ModfdRcrd>, <TermntdRcrd>, <CancRcrd>)
- Source file tracking
- Unique constraint on (isin, version_number)

### Cancellations Tracking

FULCAN files are processed into the `cancellations` table:
- Original ISIN and trading venue
- Cancellation date and reason
- Original publication date reference
- Full audit trail

This enables regulatory compliance for historical queries and audit trails per ESMA guidance.

## Examples

See `examples/` directory:

- `00_initialize_database.py` - Database initialization
- `02_index_with_filters.py` - Download and index with asset type filters
- `03_cfi_classification.py` - CFI classification and decoding
- `06_transparency_data.py` - FITRS transparency data queries

```bash
python examples/00_initialize_database.py
python examples/06_transparency_data.py
```

## Reference API

The package provides a convenient hierarchical API for querying instruments:

```python
import esma_dm as edm

# Direct ISIN lookup (callable interface)
instrument = edm.reference('EZKLV6Z6S7X8')

# Asset type-specific queries
swap_types = edm.reference.swap.types()      # All unique swap CFI codes
equity_count = edm.reference.equity.count()  # Total equity instruments
samples = edm.reference.forward.sample(10)   # 10 sample forwards

# Global statistics
summary = edm.reference.summary()            # Summary of all asset types
all_types = edm.reference.types()            # All CFI codes across types

# Subtype discovery (8 major models covering 73% of instruments)
subtypes = edm.reference.subtypes()          # Available output models
```

**Available asset types**: `equity`, `debt`, `civ`, `futures`, `options`, `swap`, `referential`, `rights`, `spot`, `forward`

## Transparency API

Query FITRS transparency data for liquidity and turnover metrics:

```python
import esma_dm as edm

# Index transparency data (download and store FITRS files)
result = edm.transparency.index('FULECR')    # Equity ISIN-level full
result = edm.transparency.index('DLTECR')    # Equity delta updates
result = edm.transparency.index('FULNCR_NYAR')  # Non-equity sub-class yearly

# Lookup by ISIN
trans = edm.transparency('GB00B1YW4409')
print(trans['liquid_market'])                # True/False
print(trans['average_daily_turnover'])       # Float value
print(trans['methodology'])                  # SINT, YEAR, ESTM, or FFWK

# Query with filters
liquid = edm.transparency.query(
    liquid_only=True,
    instrument_type='equity',
    methodology='YEAR',
    min_turnover=1_000_000
)

# Utility enums for abbreviated codes
from esma_dm.clients.fitrs import FITRSClient
fitrs = FITRSClient()

# List available codes with descriptions
methodologies = fitrs.list_methodologies()
# [{'code': 'SINT', 'description': 'Systematic Internaliser historical...'}, ...]

classifications = fitrs.list_classifications()
# [{'code': 'SHRS', 'description': 'Shares (common/ordinary shares)'}, ...]

# Get info for specific code
info = fitrs.get_methodology_info('YEAR')
# {'code': 'YEAR', 'description': 'Yearly methodology (12-month rolling period)', ...}

# Cross-table queries (FIRDS instruments + FITRS transparency in unified DB)
sql = """
SELECT i.isin, i.full_name, i.short_name, t.liquid_market, t.average_daily_turnover
FROM instruments i
JOIN transparency t ON i.isin = t.isin
WHERE t.liquid_market = TRUE AND t.methodology = 'YEAR'
"""
results = edm.transparency.client.data_store.query(sql)
```

**Features**:
- Unified database: FIRDS and FITRS transparency data in the same esma_{mode}.duckdb
- Support for all 6 file types: FULECR, FULNCR, DLTECR, DLTNCR, FULNCR_NYAR, FULNCR_SISC
- Full MiFIR compliance: most relevant market, application periods, LIS/SSTI thresholds
- Sub-class level results with segmentation criteria (30+ criteria types)
- Utility enums with descriptions for abbreviated codes (methodologies, classifications, criteria)
- Filter by liquidity, instrument type, methodology, turnover thresholds

## Key Features

### Reference API
- **Hierarchical access**: `edm.reference.swap.types()` for asset-specific queries
- **Callable interface**: `edm.reference('ISIN')` for direct lookups
- **Type discovery**: Get all unique CFI codes per asset type
- **Subtype models**: 8 specialized output models covering 73% of instruments
- **Statistics**: Count and sample methods for each asset type

### Transparency API
- **FITRS data**: Unified storage — transparency and reference data in the same esma_{mode}.duckdb
- **Direct joins**: Query instruments joined with transparency without any ATTACH
- **Liquidity filtering**: Query by liquid_market flag and turnover thresholds
- **File support**: FULECR (equity) and FULNCR (non-equity) formats
- **Simple interface**: `edm.transparency('ISIN')` for direct lookups

### CFI Classification (ISO 10962)
- Complete implementation of ISO 10962 across all 14 categories and all 6 CFI code positions
- Modular package (`esma_dm/models/utils/cfi/`): one file per category, each with a dedicated group enum, attribute value dictionaries sourced from the ISO 10962 JSON, and `decode_attributes()` / `attribute_labels()` functions
- Central `cfi_instrument_manager.py`: `CFI` dataclass, `decode_cfi()`, `get_attribute_labels()`, `group_description()` with automatic dispatch to the correct category module
- All 14 group enums and helper functions re-exported through the package `__init__.py`; existing import paths unchanged

### DuckDB Storage
- Star schema with master instruments table + 14 asset-specific tables
- ISO 10962 compliant naming: `spot_instruments` (I), `forward_instruments` (J)
- Complete support for all 14 FIRDS asset types:
  - C: Collective Investment Vehicles
  - D: Debt Instruments (bonds, notes with interest rate fields)
  - E: Equities (shares with dividend/voting rights)
  - F: Futures (with commodity product classifications)
  - H: Non-Standard Derivatives (warrants, OTC options, swaptions)
  - I: Spot (spot contracts and indices)
  - J: Forwards (forward contracts and warrants)
  - K: Strategies (multi-leg combinations)
  - L: Financing (repos, SFTs)
  - M: Others / miscellaneous
  - O: Options (OTC with strike price variations)
  - R: Rights/Entitlements (subscription rights)
  - S: Swaps (interest rate and FX swaps)
  - T: Referential (currencies, benchmarks, indices)
- Vectorized bulk loading with pandas + DuckDB
- Foreign key relationships for data integrity
- 11 indexes for optimized query patterns
- Modular architecture: separated schema, bulk inserters, orchestration
- FIRDS and FITRS transparency data unified in esma_{mode}.duckdb

### Validation
- ISIN (ISO 6166)
- LEI (ISO 17442)
- CFI codes (ISO 10962)

## Performance

- **7.03M instruments** across all 14 FIRDS asset types
- **10.76M FITRS transparency records** (equity and non-equity)
- **Database size**: ~6.87 GB combined (FIRDS reference + FITRS transparency)
- **Query speed**: Sub-second SQL queries with indexed columns
- **Memory efficient**: Vectorized pandas operations with DuckDB bulk insert

## License

MIT

## Support

Open an issue on GitHub for questions or suggestions.



