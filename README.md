# ESMA Data Manager

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) published reference data and transparency information.

**Note**: This is an unofficial package and is not endorsed by ESMA. All data is sourced from publicly available ESMA registers.

## Features

- **FIRDS**: Financial Instruments Reference Data System with complete asset type coverage
- **10 Asset Types**: Full support for C, D, E, F, H, I, J, O, R, S instrument types
- **CFI Classification**: Complete ISO 10962 decoding with full attribute descriptions
- **High Performance**: Vectorized bulk loading at 33,000+ instruments/second
- **DuckDB Storage**: Fast analytical queries on star schema with 11 normalized tables
- **RTS 23 Compliance**: Full support for regulatory technical standards
- **SQL Interface**: Run complex queries on 2.3M+ instruments
- **Modular Architecture**: Separated schema, bulk inserters, and storage orchestration

## Installation

```bash
# Install in development mode
pip install -e .
```

## Quick Start

```python
from esma_dm import FIRDSClient

# Initialize client
firds = FIRDSClient()

# Download latest equity files
data = firds.get_latest_full_files(asset_type='E')

# Index data into DuckDB
firds.index_cached_files()

# Query reference data
instrument = firds.reference('US0378331005')
print(f"Found: {instrument['full_name']}")
```

## Complete Workflow

### 1. Installation

```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -e .
```

### 2. Initialize & Download

```python
from esma_dm import FIRDSClient

firds = FIRDSClient()

# Download latest files
firds.get_latest_full_files(asset_type='E')  # Equities

# Index into DuckDB
results = firds.index_cached_files(delete_csv=True)
print(f"Indexed {results['total_instruments']:,} instruments")
```

### 3. Query Data

```python
# Simple lookup
instrument = firds.reference('SE0000242455')

# SQL queries
df = firds.data_store.query("""
    SELECT instrument_type, COUNT(*) as count
    FROM instruments
    GROUP BY instrument_type
    ORDER BY count DESC
""")
```

## Examples

See `examples/` directory:

- `01_basic_usage.py` - Getting started
- `02_reference_lookup.py` - Query methods  
- `03_advanced_queries.py` - Complex SQL queries

```bash
python examples/01_basic_usage.py
```

## Project Structure

```
esma-dm/
├── esma_dm/
│   ├── clients/          # FIRDS, FITRS, SSR, Benchmarks
│   ├── storage/          # DuckDB and JSON backends
│   ├── models/           # Data models and mappers
│   └── utils.py
└── examples/             # Usage examples
```

## Key Features

### DuckDB Storage
- Star schema with master instruments table + 10 asset-specific tables
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



