# ESMA Data Manager

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) published reference data and transparency information.

**Note**: This is an unofficial package and is not endorsed by ESMA. All data is sourced from publicly available ESMA registers.

## Features

- **FIRDS**: Financial Instruments Reference Data System with complete asset type coverage
- **FITRS**: Financial Instruments Transparency System with equity and non-equity data
- **10 Asset Types**: Full support for C, D, E, F, H, I, J, O, R, S instrument types
- **CFI Classification**: Complete ISO 10962 decoding with full attribute descriptions
- **High Performance**: Vectorized bulk loading at 33,000+ instruments/second
- **DuckDB Storage**: Fast analytical queries on star schema with 12 normalized tables
- **Cross-Database Queries**: Join FIRDS reference data with FITRS transparency metrics
- **Normalized Design**: Separate listings table for one-to-many venue relationships
- **RTS 23 Compliance**: Full support for regulatory technical standards
- **SQL Interface**: Run complex queries on 2.3M+ instruments
- **Database Management**: Initialize, drop, and update methods for lifecycle control
- **Modular Architecture**: Separated schema, bulk inserters, and storage orchestration
- **Transparency API**: Query liquidity and turnover metrics via edm.transparency()

## Installation

```bash
# Install in development mode
pip install -e .
```

## Quick Start

```python
import esma_dm as edm

# 1. Initialize database
firds = edm.FIRDSClient()
firds.data_store.initialize(mode='current')

# 2. Download latest files (optional, if not already cached)
firds.get_latest_full_files(asset_type='E')  # Equities

# 3. Load database from CSV files
results = firds.index_cached_files()
print(f"Indexed {results['total_instruments']:,} instruments")

# 4. Query reference data
instrument = edm.reference('SE0000242455')
print(instrument['short_name'])  # SWEDBANK/SH A

# 5. Asset type queries
swap_types = edm.reference.swap.types()
print(f"Unique swap CFI codes: {len(swap_types)}")

# 6. Statistics
print(f"Total swaps: {edm.reference.swap.count():,}")
summary = edm.reference.summary()
```

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

firds = FIRDSClient()

# Initialize database (creates schema and verifies structure)
result = firds.data_store.initialize(mode='current')
print(f"Status: {result['status']}")
print(f"Tables created: {result['tables_created']}")

# If database already exists, it will verify schema without reinitializing
# To force recreation, drop first:
# firds.data_store.drop()
# firds.data_store.initialize(mode='current')
```

### 3. Download Data

Download FIRDS files from ESMA registry:

```python
# Download latest files for specific asset types
firds.get_latest_full_files(asset_type='E')  # Equities
firds.get_latest_full_files(asset_type='D')  # Debt
firds.get_latest_full_files(asset_type='S')  # Swaps

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
firds.data_store.initialize(mode='current')

# 3. Download latest data
for asset_type in ['E', 'D', 'S']:  # Example: equities, debt, swaps
    firds.get_latest_full_files(asset_type=asset_type)

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

## Examples

See `examples/` directory:

- `01_basic_usage.py` - Getting started
- `02_reference_lookup.py` - Query methods  
- `03_cfi_classification.py` - CFI classification and decoding
- `06_transparency_data.py` - FITRS transparency data and cross-database queries

```bash
python examples/01_basic_usage.py
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

# Cross-database queries (join FIRDS + FITRS)
edm.transparency.attach_firds()
sql = """
SELECT f.isin, f.full_name, t.liquid_market, t.average_daily_turnover
FROM firds.instruments f
JOIN transparency t ON f.isin = t.isin
WHERE t.liquid_market = TRUE AND t.methodology = 'YEAR'
"""
results = edm.transparency.client.data_store.query(sql)
```

**Features**:
- Separate fitrs.db database for transparency data
- Support for all 6 file types: FULECR, FULNCR, DLTECR, DLTNCR, FULNCR_NYAR, FULNCR_SISC
- Full MiFIR compliance: most relevant market, application periods, LIS/SSTI thresholds
- Sub-class level results with segmentation criteria (30+ criteria types)
- Utility enums with descriptions for abbreviated codes (methodologies, classifications, criteria)
- Cross-database queries via DuckDB ATTACH
- Filter by liquidity, instrument type, methodology, turnover thresholds

## Project Structure

```
esma-dm/
├── esma_dm/
│   ├── clients/          # FIRDS, FITRS, SSR, Benchmarks
│   ├── storage/          # DuckDB storage with modular architecture
│   │   ├── schema.py     # FIRDS table definitions (12 tables)
│   │   ├── fitrs_schema.py    # FITRS table definitions (4 tables)
│   │   ├── bulk_inserters.py  # Asset-specific bulk insert handlers
│   │   ├── duckdb_store.py    # FIRDS storage orchestration
│   │   └── fitrs_store.py     # FITRS storage with cross-database support
│   ├── models/           # Data models and CFI classification
│   │   ├── subtypes.py   # 8 specialized output models for major subtypes
│   │   └── utils/        # CFI (ISO 10962) implementation
│   ├── reference_api.py  # Hierarchical reference API
│   ├── transparency_api.py    # Transparency data API
│   └── utils.py
├── tools/                # Analysis and inspection tools
│   ├── analyze_field_coverage.py      # Compare DB vs CSV data quality
│   ├── display_database_schema.py     # Show actual DuckDB table schemas
│   └── display_schemas.py             # Show Python model schemas
└── examples/             # Usage examples
```

## Key Features

### Reference API
- **Hierarchical access**: `edm.reference.swap.types()` for asset-specific queries
- **Callable interface**: `edm.reference('ISIN')` for direct lookups
- **Type discovery**: Get all unique CFI codes per asset type
- **Subtype models**: 8 specialized output models covering 73% of instruments
- **Statistics**: Count and sample methods for each asset type

### Transparency API
- **FITRS data**: Separate database for transparency metrics
- **Cross-database queries**: Join reference data with transparency via DuckDB ATTACH
- **Liquidity filtering**: Query by liquid_market flag and turnover thresholds
- **File support**: FULECR (equity) and FULNCR (non-equity) formats
- **Simple interface**: `edm.transparency('ISIN')` for direct lookups

### CFI Classification (ISO 10962)
- Complete implementation of ISO 10962 standard
- 14 categories: E, D, C, F, O, S, H, R, I, J, K, L, T, M
- Comprehensive attribute decoders for all categories
- Category and group descriptions
- Integrated into reference data queries

### DuckDB Storage
- Star schema with master instruments table + 10 asset-specific tables
- ISO 10962 compliant naming: `spot_instruments` (I), `forward_instruments` (J)
- Complete support for all FIRDS asset types:
  - C: Collective Investment Vehicles
  - D: Debt Instruments (bonds, notes with interest rate fields)
  - E: Equities (shares with dividend/voting rights)
  - F: Futures (with commodity product classifications)
  - H: Referential Instruments (options with full strike price variations)
  - I: Spot (spot contracts and indices)
  - J: Forwards (forward contracts and warrants)
  - O: Options (OTC with strike price variations)
  - R: Rights/Entitlements (warrants, subscription rights)
  - S: Swaps (interest rate and FX swaps)
- Vectorized bulk loading with pandas + DuckDB
- Foreign key relationships for data integrity
- 11 indexes for optimized query patterns
- Modular architecture: separated schema, bulk inserters, orchestration
- Separate FITRS database (fitrs.db) for transparency data

### Validation
- ISIN (ISO 6166)
- LEI (ISO 17442)
- CFI codes (ISO 10962)

## Performance

- **2.37M instruments** indexed in 71 seconds
- **33,374 instruments/second** average rate
- **Asset-specific rates**: 142K inst/sec (CIVs), 83K inst/sec (equities), 61K inst/sec (debt)
- **Database size**: 625 MB for 2.37M instruments
- **Query speed**: Sub-second SQL queries with indexed columns
- **Memory efficient**: Vectorized pandas operations with DuckDB bulk insert

## License

MIT

## Support

Open an issue on GitHub for questions or suggestions.



