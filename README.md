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
import esma_dm as edm

# Simple reference lookup
instrument = edm.reference('SE0000242455')
print(instrument['short_name'])  # SWEDBANK/SH A

# Asset type queries
swap_types = edm.reference.swap.types()
print(f"Unique swap CFI codes: {len(swap_types)}")

# Asset type statistics
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

## Examples

See `examples/` directory:

- `01_basic_usage.py` - Getting started
- `02_reference_lookup.py` - Query methods  
- `03_cfi_classification.py` - CFI classification and decoding

```bash
python examples/01_basic_usage.py
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
```

**Available asset types**: `equity`, `debt`, `civ`, `futures`, `options`, `swap`, `referential`, `rights`, `spot`, `forward`

## Project Structure

```
esma-dm/
├── esma_dm/
│   ├── clients/          # FIRDS, FITRS, SSR, Benchmarks
│   ├── storage/          # DuckDB storage with modular architecture
│   │   ├── schema.py     # Table definitions (11 tables)
│   │   ├── bulk_inserters.py  # Asset-specific bulk insert handlers
│   │   └── duckdb_store.py    # Storage orchestration
│   ├── models/           # Data models and CFI classification
│   │   └── utils/        # CFI (ISO 10962) implementation
│   ├── reference_api.py  # Hierarchical reference API
│   └── utils.py
└── examples/             # Usage examples
```

## Key Features

### Reference API
- **Hierarchical access**: `edm.reference.swap.types()` for asset-specific queries
- **Callable interface**: `edm.reference('ISIN')` for direct lookups
- **Type discovery**: Get all unique CFI codes per asset type
- **Statistics**: Count and sample methods for each asset type

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



