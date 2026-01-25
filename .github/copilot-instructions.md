# GitHub Copilot Instructions for esma-dm

## Project Overview
Python package for accessing ESMA (European Securities and Markets Authority) published data including FIRDS, FITRS, SSR, and Benchmarks. The package downloads XML files from ESMA registers, parses them into normalized models, and provides both DuckDB storage with SQL queries and direct Python API access.

## Architecture Overview

## Architecture Overview

### Modular Design (2026-01-11 Refactoring)

The package follows a clean, modular architecture with four main layers:

1. **Client Layer** (`esma_dm/clients/`): Download and parse ESMA XML/CSV files
   - `firds/`: Modular FIRDS client (6 focused modules)
     - `client.py`: Main orchestrator (265 lines)
     - `downloader.py`: File download and caching (280 lines) 
     - `parser.py`: CSV parsing and mapping (200 lines)
     - `delta_processor.py`: Delta file processing (150 lines)
     - `enums.py`: Type definitions (45 lines)
     - `models.py`: Data models (120 lines)
   - `FITRSClient`: Transparency/liquidity metrics
   - `BenchmarksClient`, `SSRClient`: Other ESMA datasets

2. **Storage Layer** (`esma_dm/storage/`): DuckDB implementation with organized structure
   - `duckdb/`: Modular DuckDB backend (5 focused modules)
     - `__init__.py`: DuckDBStorage orchestrator (130 lines)
     - `connection.py`: Database connection management (140 lines)
     - `operations.py`: Bulk insert/update operations (285 lines)
     - `queries.py`: Retrieval and search queries (350 lines)
     - `versioning.py`: Delta processing and version management (250 lines)
   - `schema/`: Table definitions organized by purpose
   - `bulk/`: Vectorized bulk loading operations
   - `fitrs/`: FITRS-specific storage components

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
- Use single transaction per asset type (see `bulk_inserters.py` pattern)
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
- `esma_dm/clients/firds.py`: Main client, 1116 lines, all download/parse logic
- `esma_dm/storage/duckdb_store.py`: Storage orchestrator, mode selection, bulk operations
- `esma_dm/storage/schema.py`: 13 table definitions, index creation
- `esma_dm/storage/bulk_inserters.py`: Vectorized insert logic per asset type
- `esma_dm/models/mapper.py`: Field mapping, model selection
- `esma_dm/models/utils/cfi.py`: ISO 10962 CFI decoding
- `esma_dm/utils/validators.py`: ISO standard validators (ISIN, LEI, CFI, MIC)
- `esma_dm/utils/constants.py`: ESMA URLs, file patterns, defaults
- `examples/02_index_with_filters.py`: Core workflow demonstration

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
