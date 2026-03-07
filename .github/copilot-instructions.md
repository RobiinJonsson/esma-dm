# GitHub Copilot Instructions for esma-dm

## Project Overview
Python package for accessing ESMA (European Securities and Markets Authority) published data including FIRDS, FITRS, SSR, and Benchmarks. The package downloads XML files from ESMA registers, parses them into normalized models, and provides both DuckDB storage with SQL queries and direct Python API access.

## Architecture Overview

### Modular Design (2026-01-11 Refactoring)

The package follows a clean, modular architecture with four main layers:

1. **Client Layer** (`esma_dm/clients/`): Download and parse ESMA XML/CSV files
   - `firds/`: Modular FIRDS client (6 focused modules)
     - `client.py`: Main orchestrator (301 lines)
     - `downloader.py`: File download and caching (462 lines) 
     - `parser.py`: CSV parsing and mapping (374 lines)
     - `delta_processor.py`: Delta file processing (164 lines)
     - `enums.py`: Type definitions (71 lines)
     - `models.py`: Data models (53 lines)
   - `fitrs.py`: FITRS client (single flat module, 673 lines)
     - `FITRSClient`: Main class — downloads transparency files from ESMA FITRS SOLR endpoint
     - Key methods: `get_file_list()`, `get_latest_full_files(asset_type, instrument_type)`, `index_transparency_data(file_type, asset_type)`, `transparency(isin)`, `query_transparency(...)`, `query_subclass_transparency(...)`
     - File type routing: `FULECR`/`FULNCR`/`DLTECR`/`DLTNCR` → `transparency` table; `FULNCR_NYAR`/`FULNCR_SISC` → `subclass_transparency` table
     - NOTE: `clients/fitrs/` directory exists but is empty — client lives in flat `clients/fitrs.py`
   - `BenchmarksClient`, `SSRClient`: Other ESMA datasets

2. **Storage Layer** (`esma_dm/storage/`): DuckDB implementation with organized structure
   - `duckdb/`: Modular DuckDB backend (5 focused modules)
     - `__init__.py`: DuckDBStorage orchestrator (160 lines)
     - `connection.py`: Database connection management (175 lines)
     - `operations.py`: Bulk insert/update operations (402 lines)
     - `queries.py`: Retrieval and search queries (530 lines)
     - `versioning.py`: Delta processing and version management (342 lines)
   - `schema/`: Table definitions organized by purpose (in operations.py)
   - `bulk/`: Vectorized bulk loading operations (in operations.py)
   - `fitrs/store.py`: `FITRSStorage` — DuckDB backend for transparency data
     - Default DB path: `downloads/fitrs.db` (note: not under `downloads/data/fitrs/`)
     - Tables: `transparency` (29 columns, ISIN-level), `subclass_transparency`, `equity_transparency`, `non_equity_transparency`, `transparency_metadata`
     - Key methods: `insert_transparency_data(df, file_type)`, `insert_subclass_transparency_data(df, file_type)`, `get_transparency(isin)`, `attach_firds_database(path)`
   - `schema/fitrs_schema.py`: All FITRS table DDL and schema metadata (260 lines)

3. **Utilities Layer** (`esma_dm/utils/`): Shared components eliminating duplication
   - `validators.py`: ISO standard validators (ISIN/ISO 6166, LEI/ISO 17442, CFI/ISO 10962, MIC/ISO 10383)
   - `constants.py`: ESMA URL constants, file patterns, default settings
   - `query_builder.py`: Reusable SQL query patterns for database operations
   - `shared_utils.py`: Common utilities for file operations and XML parsing
   - `__init__.py`: Unified export interface for all utilities

4. **API Layer** (`esma_dm/`): High-level Pythonic access patterns
   - `reference_api.py`: `edm.reference('ISIN')` and `edm.reference.swap.types()`
   - `transparency_api.py`: `edm.transparency('ISIN')` for FITRS data
   - Cross-database joins via DuckDB ATTACH

### Benefits of Modular Architecture

- **Maintainability**: Each module has a single, clear responsibility
- **Testability**: Components can be tested independently with focused test suites
- **Reusability**: Utilities eliminate code duplication across the entire codebase
- **Extensibility**: Easy to add new data sources, storage backends, or query patterns
- **Type Safety**: Full type hints and validation throughout
- **Performance**: QueryBuilder centralizes SQL optimization patterns

### Mode-Based Operation
Critical design decision (see CHANGELOG 2026-01-11): FIRDSClient has two operational modes that use different databases and schemas:

```python
# Current mode (default): Latest snapshots only, 9 columns, fast queries
firds = FIRDSClient(mode='current')  # → firds_current.duckdb

# History mode: Full ESMA Section 8.2 compliance, 17 columns, version tracking
firds = FIRDSClient(mode='history')  # → firds_history.duckdb
firds.process_delta_files()  # Only available in history mode
```

**When to use each mode:**
- Current: Production queries on latest data, storage efficiency priority
- History: Regulatory compliance, audit trails, point-in-time queries

### Data Flow
1. **Download**: `get_latest_full_files(asset_type='E')` → CSV in `downloads/data/firds/`
2. **Parse**: `InstrumentMapper` → Asset-specific model (EquityInstrument, DebtInstrument, etc.)
3. **Vectorize**: `BulkInserter.prepare_vectors()` → NumPy arrays grouped by asset type
4. **Bulk Insert**: `BulkInserter.insert_batch()` → Single transaction per asset type
5. **Query**: SQL or API (`edm.reference('ISIN')`)

### CFI-Driven Model Selection
CFI code first character determines model class:
```python
# E → EquityInstrument, D → DebtInstrument, S → SwapInstrument, etc.
from esma_dm.models.mapper import InstrumentMapper
instrument = InstrumentMapper.map_to_instrument(row_data)  # Auto-selects class
```

See `esma_dm/models/utils/cfi.py` for full ISO 10962 decoding (6-char CFI → human descriptions).

## Code Style Rules

### Documentation
- No emojis in any code, comments, or documentation
- Professional and concise language only
- Use clear, technical descriptions
- Follow Google-style docstrings for Python

### Markdown Files
- Only two .md files allowed at project root: README.md and CHANGELOG.md
- Folders may have their own README.md only if necessary
- CHANGELOG.md must have timestamped entries (YYYY-MM-DD format)
- README.md describes package usage and API

### Python Code
- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Use dataclasses for structured data (see `esma_dm/models/`)
- Use Enums for fixed sets of values (AssetType, FileType, OptionType, etc.)
- **Use centralized utilities**: Import validators from `esma_dm.utils.validators`, constants from `esma_dm.utils.constants`
- Validate inputs using proper validation methods
- Keep methods focused and single-purpose
- **No hardcoded URLs**: Use constants from `esma_dm.utils.constants`
- **No duplicate code**: Extract common patterns to utility modules

### Data Standards
- Follow ISO standards: ISO 6166 (ISIN), ISO 17442 (LEI), ISO 10962 (CFI)
- Follow RTS 23 specifications for FIRDS data
- Follow ESMA65-8-5014 Section 8 for historical tracking (history mode only)
- Use pandas DataFrames for tabular data
- Parse dates to datetime objects
- Normalize data to typed models where appropriate

### Naming Conventions
- Classes: PascalCase (e.g., FIRDSClient)
- Functions/methods: snake_case (e.g., get_file_list)
- Constants: UPPER_SNAKE_CASE (e.g., BASE_URL)
- Private methods: prefix with underscore (e.g., _parse_xml)

### Import Organization
1. Standard library imports
2. Third-party imports (pandas, requests, etc.)
3. Local imports
4. Use absolute imports, not relative

### Error Handling
- Raise exceptions for invalid inputs
- Log errors with appropriate level (use logging module)
- Provide helpful error messages
- Clean up resources in finally blocks

## Utility Modules

### Validators (`esma_dm/utils/validators.py`)
ISO standard validation for financial identifiers:
```python
from esma_dm.utils import validate_isin, validate_lei, validate_cfi, validate_mic

# Validate ISIN (ISO 6166)
if validate_isin('US0378331005'):
    print("Valid ISIN")

# Validate LEI (ISO 17442)
if validate_lei('549300VALTPVHYSYMH70'):
    print("Valid LEI")

# Validate CFI (ISO 10962)
if validate_cfi('ESVUFR'):
    print("Valid CFI")

# Validate MIC (ISO 10383) - supports both X-prefixed and other formats
if validate_mic('XNYS'):  # NYSE
    print("Valid MIC")
if validate_mic('FRAB'):  # Brussels (doesn't start with X)
    print("Valid MIC")

# Multi-type validation
is_valid, error = validate_instrument_identifier('US0378331005', 'ISIN')
```

### Constants (`esma_dm/utils/constants.py`)
Centralized ESMA register URLs and configuration values:
```python
from esma_dm.utils.constants import (
    FIRDS_SOLR_URL,        # FIRDS endpoint
    FITRS_SOLR_URL,        # FITRS endpoint
    DVCAP_SOLR_URL,        # DVCAP endpoint
    SSR_SOLR_URL,          # SSR endpoint
    ASSET_TYPE_CODES,      # CFI asset type mapping
    DATABASE_MODES,        # Valid mode values
    FILE_TYPE_PATTERNS     # Filename patterns
)

# Use in clients
class MyClient:
    BASE_URL = FIRDS_SOLR_URL  # Instead of hardcoded URL
```

### QueryBuilder (`esma_dm/utils/query_builder.py`)
Reusable SQL query patterns eliminating code duplication:
```python
from esma_dm.utils import QueryBuilder

# Create mode-aware query builder
qb = QueryBuilder('current')  # or 'history'

# Generate consistent SQL patterns
isin_query = qb.get_instrument_by_isin('US0378331005')
search_query = qb.search_instruments(limit=10)
stats_query = qb.get_stats_by_asset_type()

# Bulk insert patterns
insert_query = qb.bulk_insert_instruments(['isin', 'cfi_code', 'full_name'])

# Asset type validation
if qb.validate_asset_type('E'):
    asset_query = qb.get_asset_specific_details('E', 'US0378331005')
```

### Configuration (`esma_dm/config.py`)
Enhanced with utility integration:
```python
from esma_dm.config import Config

config = Config(mode='current')

# Mode validation (raises ValueError if invalid)
config.mode  # Must be 'current' or 'history'

# Database path helper
db_path = config.get_database_path('firds', 'current')
# Returns: downloads/data/firds/firds_current.duckdb

# ESMA URL access
config.FIRDS_BASE_URL  # From constants module
config.FITRS_BASE_URL
```

## Critical Development Workflows

### Initial Setup
```bash
# 1. Install in editable mode (use project's venv: esma-venv)
pip install -e .

# 2. Initialize database (run from examples/ or tests/)
python examples/00_initialize_database.py
# OR in code:
from esma_dm import FIRDSClient  # Use recommended import (from clients)
firds = FIRDSClient(mode='current')
firds.data_store.initialize()

# 3. Download and index latest data
firds.get_latest_full_files(asset_type='E')  # Uses cache by default
firds.index_cached_files()  # Bulk load to DuckDB
```

### Migration from Legacy Module
**DEPRECATED**: `esma_dm.firds` module is deprecated. Use `esma_dm.clients.firds` instead.

```python
# ❌ OLD (deprecated, shows warnings)
from esma_dm.firds import FIRDSClient

# ✅ NEW (recommended)
from esma_dm import FIRDSClient  # Imports from clients automatically

# ✅ EXPLICIT 
from esma_dm.clients.firds import FIRDSClient
```

**Migration benefits**: Mode-based operation, enhanced caching, delta processing, better error handling.

### Key Project Structure

Current workspace organization (as of 2026-02-23):
```
esma_dm/
├── clients/
│   ├── firds/              # 6 focused modules (1,452 lines total)
│   │   ├── client.py      # Main orchestrator
│   │   ├── downloader.py  # File download and caching
│   │   ├── parser.py      # CSV parsing and mapping
│   │   ├── delta_processor.py  # Delta file processing
│   │   ├── enums.py       # Type definitions
│   │   └── models.py      # Data models
│   └── fitrs.py            # FITRSClient (flat module, 673 lines)
├── storage/
│   ├── duckdb/             # 5 focused modules (1,609 lines total)
│   │   ├── __init__.py    # DuckDBStorage orchestrator
│   │   ├── connection.py  # Database management
│   │   ├── operations.py  # Bulk insert/update operations
│   │   ├── queries.py     # Retrieval and search queries
│   │   └── versioning.py  # Delta processing and version management
│   ├── fitrs/
│   │   └── store.py       # FITRSStorage DuckDB backend
│   └── schema/
│       └── fitrs_schema.py  # FITRS table DDL
├── file_manager/
│   └── fitrs/              # FITRSFileManager (CLI download/cache layer)
│       └── manager.py
├── models/
│   └── transparency_enums.py  # Methodology, InstrumentClassification, FileType, SegmentationCriteria
├── utils/                  # Shared utilities eliminating duplication
│   ├── validators.py      # ISO standard validators
│   ├── constants.py       # ESMA URLs and configuration
│   ├── query_builder.py   # Reusable SQL patterns
│   └── shared_utils.py    # Common utilities
├── reference_api.py        # High-level FIRDS query interface (314 lines)
└── transparency_api.py     # Thin facade over FITRSClient for edm.transparency
```

### Testing
- Use pytest: `pytest tests/` (tests use pytest fixtures and assertions)
- Mock external API calls with `unittest.mock.patch`
- See `tests/test_delta_processing.py` for version management tests
- See `tests/test_firds.py` for validation tests

### Database Management
```python
# Drop database (requires confirmation)
firds.data_store.drop(confirm=True)

# Query statistics
stats = firds.get_store_stats()  # Returns instrument counts by type

# Direct SQL (for complex queries)
result = firds.data_store.con.execute("SELECT * FROM instruments WHERE cfi_code LIKE 'E%'").fetchdf()
```

### FITRS Transparency Workflow

```python
from esma_dm import FITRSClient
fitrs = FITRSClient()

# List available files from ESMA FITRS register
files_df = fitrs.get_file_list()

# Download and cache files (equity full transparency)
fitrs.get_latest_full_files(asset_type='E')

# Full ETL pipeline: list → filter → download → insert into DuckDB
fitrs.index_transparency_data(file_type='FULECR', asset_type='E', latest_only=True)

# Also index non-equity transparency
fitrs.index_transparency_data(file_type='FULNCR', latest_only=True)

# Sub-class transparency (NYAR = new assessment per asset class, SISC = sub-instrument sub-class)
fitrs.index_transparency_data(file_type='FULNCR_NYAR')
fitrs.index_transparency_data(file_type='FULNCR_SISC')

# Point lookup
transparency = fitrs.transparency('GB00B1YW4409')  # Returns dict

# Parameterised query → DataFrame
df = fitrs.query_transparency(liquid_market=True, methodology='SINT', limit=100)
df_sub = fitrs.query_subclass_transparency(asset_class='SHRS')

# Cross-database JOIN with FIRDS
fitrs.data_store.attach_firds_database('downloads/data/firds/firds_current.duckdb')
```

**CLI commands** (use `FITRSFileManager`, file-oriented, independent of DB):
```bash
python -m esma_dm fitrs list                   # Lists ESMA register files (paginated Rich table)
python -m esma_dm fitrs download               # Downloads latest full files (equity + non-equity)
python -m esma_dm fitrs cache                  # Lists locally cached CSV files by type/instrument
python -m esma_dm fitrs stats                  # Cache statistics: counts by file type and instrument
python -m esma_dm fitrs types                  # Prints all file type codes with descriptions
python -m esma_dm fitrs fields <path>          # Reads CSV header and prints column names + dtypes
python -m esma_dm fitrs head <path>            # Prints first N rows of a CSV file
```

**FITRS file type taxonomy**:
- `FULECR`: Full Equity Calculation Results (ISIN-level)
- `FULNCR`: Full Non-Equity Calculation Results (ISIN-level)
- `DLTECR`: Delta Equity Calculation Results (ISIN-level)
- `DLTNCR`: Delta Non-Equity Calculation Results (ISIN-level)
- `FULNCR_NYAR`: Full Non-Equity sub-class results (new assessment per asset class)
- `FULNCR_SISC`: Full Non-Equity sub-class results (sub-instrument sub-class)

**Transparency enums** (from `esma_dm.models.transparency_enums`):
- `Methodology`: `SINT`, `YEAR`, `ESTM`, `FFWK`
- `InstrumentClassification`: `SHRS`, `DPRS`, `ETFS`, `OTHR`
- `FileType`: classmethod helpers `is_equity()`, `is_non_equity()`, `is_subclass()`, `is_delta()`
- `SegmentationCriteria`: 20+ sub-class segmentation criteria codes

**High-level API** (via `edm.transparency`):
```python
import esma_dm as edm

# Point lookup
instrument = edm.transparency('GB00B1YW4409')

# Index methods
edm.transparency.index('FULECR', asset_type='E')
edm.transparency.query(liquid_market=True)
edm.transparency.query_subclass(asset_class='SHRS')
edm.transparency.attach_firds(path='downloads/data/firds/firds_current.duckdb')
```

**FITRS architecture note — two parallel file systems**:
- CLI path: `FITRSFileManager` (`file_manager/fitrs/`) — `fitrs download` calls `Utils.download_and_parse_file()` internally, saving ``{stem}_data.csv`` to `downloads/data/fitrs/`; old files for the same type and earlier dates are removed automatically
- API path: `FITRSClient` also uses `Utils.download_and_parse_file()` with the same `downloads/data/fitrs/` cache
- Both paths use identical file naming (`*_data.csv`) — files produced by one are visible to the other
- DB file: `downloads/fitrs.db` (root `downloads/`, not `downloads/data/fitrs/fitrs.db`)
- FITRS zip files from ESMA contain XML (not CSV); `Utils.download_and_parse_file` handles extraction and XML parsing

### Delta File Processing (History Mode Only)
```python
firds = FIRDSClient(mode='history')
firds.data_store.initialize(mode='history')

# Initial full load
firds.get_latest_full_files(asset_type='E')
firds.index_cached_files()

# Process daily deltas (NEW, MODIFIED, TERMINATED, CANCELLED records)
stats = firds.process_delta_files(
    asset_type='E',
    date_from='2026-01-04',
    date_to='2026-01-11'
)
```

Record types follow ESMA Section 8.2:
- `NEW`: Insert new instrument (increments version_number if ISIN exists)
- `MODIFIED`: Close previous version (set valid_to_date), insert new version
- `TERMINATED`: Archive and mark as terminated
- `CANCELLED`: Move to cancellations table

## Project-Specific Patterns

### Asset Type Filtering
All methods support 10 asset types (CFI first character):
```python
# Download equities only
firds.get_latest_full_files(asset_type='E')

# Query methods filter by asset_type
edm.reference.equity.count()  # E instruments
edm.reference.swap.types()    # S instruments with CFI descriptions
```

### Caching Strategy
Default behavior (added 2026-01-11): Use cached files for fast iteration
```python
# Uses cache (fast, good for development)
firds.get_latest_full_files(asset_type='E')

# Force fresh download (explicit when needed)
firds.get_latest_full_files(asset_type='E', update=True)
```

### Model Mapping Pattern
InstrumentMapper uses field name normalization:
```python
# Handles both RefData_FinInstrmGnlAttrbts_Id and FinInstrmGnlAttrbts_Id
# Maps raw XML columns → model attributes via COMMON_FIELD_MAP, DEBT_FIELD_MAP, etc.
# See esma_dm/models/mapper.py lines 20-90 for full mappings
```

### Validation Methods
Implement as static methods on client classes:
```python
@staticmethod
def validate_isin(isin: str) -> bool:
    """Validate ISIN format (ISO 6166)."""
    if not isinstance(isin, str) or len(isin) != 12:
        return False
    # ... check structure
    return True
```

### API Design Pattern
Two-tier API: functional shorthand + class-based queries
```python
# Shorthand for single ISIN lookup
import esma_dm as edm
instrument = edm.reference('SE0000242455')

# Class-based for complex queries
result = edm.reference.swap.types()  # Returns DataFrame with CFI descriptions
count = edm.reference.equity.count()
```

## Common Pitfalls

### Mode Confusion
- Don't call `process_delta_files()` in current mode (raises error)
- Don't query historical fields (version_number, valid_from_date) in current mode (columns don't exist)
- Always specify mode explicitly if using history features

### File Path Handling
- Use `self.config.downloads_path` not hardcoded paths
- Files are cached in `downloads/data/firds/` (FIRDS) or `downloads/data/fitrs/` (FITRS)
- Database files are in same directories as data

### CFI Code Dependencies
- Always use `CFI` class from `esma_dm/models/utils/cfi.py` for CFI decoding
- CFI first character determines table routing (equity, debt, swap, etc.)
- Invalid CFI codes should log warnings but not crash bulk loading

### Bulk Insert Performance
- Group instruments by asset type BEFORE calling inserter
- Use single transaction per asset type (see `operations.py` pattern)
- Don't insert one-by-one in loops (30x slower than vectorized approach)

## Testing Commands
```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_delta_processing.py -v

# Run with coverage
pytest --cov=esma_dm tests/

# Example scripts validate core workflows
python examples/02_index_with_filters.py
python examples/03_cfi_classification.py
```

## Key Files Reference

### FIRDS
- `esma_dm/clients/firds/client.py`: Main client, 301 lines, all download/parse logic
- `esma_dm/storage/duckdb/__init__.py`: Storage orchestrator, mode selection, bulk operations
- `esma_dm/storage/duckdb/operations.py`: Table definitions, bulk insert operations
- `esma_dm/storage/duckdb/queries.py`: Instrument retrieval and search logic
- `esma_dm/models/mapper.py`: Field mapping, model selection
- `esma_dm/models/utils/cfi.py`: ISO 10962 CFI decoding
- `examples/02_index_with_filters.py`: Core workflow demonstration

### FITRS
- `esma_dm/clients/fitrs.py`: FITRSClient, 673 lines, full ETL and query logic
- `esma_dm/storage/fitrs/store.py`: FITRSStorage DuckDB backend, 400 lines
- `esma_dm/storage/schema/fitrs_schema.py`: FITRS table DDL (transparency, subclass_transparency), 260 lines
- `esma_dm/models/transparency_enums.py`: Methodology, InstrumentClassification, FileType, SegmentationCriteria enums, 289 lines
- `esma_dm/file_manager/fitrs/manager.py`: FITRSFileManager (CLI file listing/download/cache)
- `esma_dm/transparency_api.py`: Thin facade for edm.transparency proxy
- `esma_dm/cli/fitrs.py`: Click CLI commands (fitrs list/download/cache/stats/types/fields/head), 467 lines
- `examples/06_transparency_data.py`: FITRS ETL and query demonstration
- `examples/07_transparency_enums.py`: Transparency enum usage

### Shared
- `esma_dm/utils/validators.py`: ISO standard validators (ISIN, LEI, CFI, MIC)
- `esma_dm/utils/constants.py`: ESMA URLs, file patterns, defaults

## What to Avoid
- Emojis anywhere in the project
- Verbose or chatty language
- Multiple markdown documentation files
- Hardcoded paths or credentials
- Catching exceptions without logging
- Methods that do too many things
- Magic numbers without constants
- Unclear variable names
- Sequential inserts instead of bulk operations
- Mixing current/history mode operations
- **Using deprecated esma_dm.firds module** (use esma_dm.clients.firds or esma_dm import)
