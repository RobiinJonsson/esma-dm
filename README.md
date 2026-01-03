# ESMA Data Manager

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) published reference data and transparency information.

## Features

- **FIRDS**: Financial Instruments Reference Data System
- **DuckDB Storage**: Fast analytical queries on normalized data
- **RTS 23 Compliance**: Full support for regulatory technical standards
- **SQL Interface**: Run complex queries on instrument data
- **Normalized Models**: Structured dataclasses for all instrument types

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
- Millions of instruments
- Fast SQL queries
- Cloud storage support (Azure, S3)

### Normalized Data Models
- EquityInstrument
- DebtInstrument
- DerivativeInstrument
- Based on RTS 23 standards

### Validation
- ISIN (ISO 6166)
- LEI (ISO 17442)
- CFI codes (ISO 10962)

## Performance

- Index 1M instruments in ~20 min
- Instant lookups
- Sub-second SQL queries
- 1.7 GB for 1M instruments

## License

MIT

## Support

Open an issue on GitHub for questions or suggestions.



