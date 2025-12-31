# ESMA Data Manager (esma-dm)

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) published reference data and transparency information.

## Features

**Modular Design** - Separate clients for different ESMA datasets:
- **FIRDS** - Financial Instruments Reference Data System
- **FITRS** - Financial Instruments Transparency System
- **SSR** - Short Selling Regulation exempted shares
- **Benchmarks** - Benchmark data

**RTS 23 Compliant** - Implements specifications from Commission Delegated Regulation (EU) supplementing MiFIR

**Type-Safe** - Comprehensive enums and dataclasses for standardized classifications

**Validation Utilities** - Built-in validators for ISIN (ISO 6166), LEI (ISO 17442), and CFI (ISO 10962) codes

**Performance Optimized** - Built-in caching, batch operations, efficient XML parsing

## Installation

```bash
pip install esma-dm
```

### Development Installation

```bash
git clone https://github.com/yourusername/esma-dm.git
cd esma-dm
pip install -e .
```

## Quick Start

### FIRDS - Reference Data

```python
from esma_dm import FIRDSClient, AssetType

# Initialize client
firds = FIRDSClient(date_from='2024-01-01')

# Get reference data for a single ISIN
ref = firds.reference('US0378331005')
if ref is not None:
    print(f"Name: {ref['FullNm']}")
    print(f"CFI: {ref['ClssfctnTp']}")
    print(f"Currency: {ref['NtnlCcy']}")

# Get all available files
files = firds.get_file_list()

# Get latest equity instruments
equities = firds.get_latest_full_files(asset_type='E')

# Get multiple ISINs with validation
isins = ['GB00B1YW4409', 'US0378331005']
valid_isins = [i for i in isins if FIRDSClient.validate_isin(i)]
instruments = firds.get_instruments(valid_isins)

# Track changes with delta files
changes = firds.get_delta_files(
    asset_type='E',
    date_from='2024-12-01',
    date_to='2024-12-31'
)
```

### FITRS - Transparency Data

```python
from esma_dm import FITRSClient

# Initialize client
fitrs = FITRSClient(date_from='2024-01-01')

# Get equity transparency data
transparency = fitrs.get_latest_full_files(
    asset_type='E',
    instrument_type='equity'
)

# Get DVCAP (Double Volume Cap) data
dvcap = fitrs.get_dvcap_latest()
```

### SSR - Short Selling Regulation

```python
from esma_dm import SSRClient

# Initialize client
ssr = SSRClient()

# Get currently active exemptions
active = ssr.get_exempted_shares(today_only=True)

# Get exemptions by country
uk_exemptions = ssr.get_exempted_shares_by_country('GB')
```

## FIRDS Advanced Features

### File Filtering

```python
from esma_dm import FIRDSClient

firds = FIRDSClient()

# Filter by file type
fulins = firds.get_file_list(file_type='FULINS')  # Full snapshots
dltins = firds.get_file_list(file_type='DLTINS')  # Delta/incremental

# Filter by asset type
equity_files = firds.get_file_list(asset_type='E')

# Combined filters
equity_fulins = firds.get_file_list(file_type='FULINS', asset_type='E')
```

### Structured Metadata

```python
from esma_dm import FIRDSClient, FIRDSFile

firds = FIRDSClient()

# Get structured file metadata
files = firds.get_files_metadata(file_type='FULINS', asset_type='E')

for f in files:
    print(f.file_name)        # FULINS_E_20241231_1of2.zip
    print(f.asset_type)       # E
    print(f.date_extracted)   # 20241231
    print(f.part_number)      # 1
    print(f.total_parts)      # 2
```

### Validation Utilities

```python
from esma_dm import FIRDSClient

# Validate ISIN (ISO 6166)
assert FIRDSClient.validate_isin('US0378331005')  # True
assert not FIRDSClient.validate_isin('INVALID')   # False

# Validate LEI (ISO 17442)
assert FIRDSClient.validate_lei('549300VALTPVHYSYMH70')  # True

# Validate CFI (ISO 10962)
assert FIRDSClient.validate_cfi('ESVUFR')  # True
```

## Data Models

### Normalized Reference Data

The package includes normalized data models for all asset types:

```python
from esma_dm.models import InstrumentMapper

# Parse FIRDS data to typed models
mapper = InstrumentMapper()
instruments = [mapper.from_row(row) for _, row in df.iterrows()]

# Access typed attributes
for instrument in instruments:
    if instrument.is_debt():
        print(f"{instrument.isin}: {instrument.maturity_date}")
    elif instrument.is_equity():
        print(f"{instrument.isin}: {instrument.has_voting_rights}")
```

### Schema Introspection

```python
from esma_dm.models import DebtInstrument, EquityInstrument

# Get schema information
debt_schema = DebtInstrument.get_schema()
for field, info in debt_schema.items():
    print(f"{field}: {info['type']} - {info['description']}")
```

## Enums and Classifications

### Asset Types (ISO 10962 CFI)

```python
from esma_dm import AssetType

AssetType.EQUITY                # "E" - Equities
AssetType.DEBT                  # "D" - Debt instruments
AssetType.FUTURES               # "F" - Futures
AssetType.OPTIONS               # "I" - Options
AssetType.SWAPS                 # "S" - Swaps
AssetType.COLLECTIVE_INVESTMENT # "C" - Collective investment vehicles
AssetType.RIGHTS                # "H" - Rights, warrants
AssetType.STRATEGIES            # "J" - Strategies, multi-leg
AssetType.OTHERS                # "O" - Others
AssetType.REFERENTIAL           # "R" - Referential instruments
```

### Commodity Classifications

```python
from esma_dm import CommodityBaseProduct

CommodityBaseProduct.AGRI  # Agricultural
CommodityBaseProduct.NRGY  # Energy
CommodityBaseProduct.METL  # Metals
CommodityBaseProduct.EMIS  # Emissions
```

### Option Classifications

```python
from esma_dm import OptionType, ExerciseStyle, DeliveryType

# Option types
OptionType.CALL  # Call option
OptionType.PUT   # Put option

# Exercise styles
ExerciseStyle.EURO  # European
ExerciseStyle.AMER  # American
ExerciseStyle.BRMN  # Bermudan
ExerciseStyle.ASIA  # Asian

# Delivery types
DeliveryType.PHYS  # Physical delivery
DeliveryType.CASH  # Cash settlement
```

### Bond Seniority

```python
from esma_dm import BondSeniority

BondSeniority.SNDB  # Senior debt
BondSeniority.MZZD  # Mezzanine
BondSeniority.SBOD  # Subordinated
BondSeniority.JUND  # Junior
```

## Configuration

### Default Configuration

```python
from esma_dm import FIRDSClient

# Uses default configuration (./downloads/data)
firds = FIRDSClient()
```

### Custom Configuration

```python
from esma_dm import FIRDSClient, Config
from pathlib import Path

config = Config(
    downloads_path=Path("/custom/path"),
    cache_enabled=True,
    log_level="INFO"
)

firds = FIRDSClient(config=config)
```

### Environment Variables

```bash
export ESMA_DM_DOWNLOADS_PATH="/custom/path"
export ESMA_DM_CACHE_ENABLED="true"
export ESMA_DM_LOG_LEVEL="INFO"
```

```python
from esma_dm import Config, FIRDSClient

config = Config.from_env()
firds = FIRDSClient(config=config)
```

## Project Structure

```
esma-dm/
├── esma_dm/
│   ├── __init__.py
│   ├── config.py
│   ├── utils.py
│   ├── firds.py
│   ├── fitrs.py
│   ├── ssr.py
│   ├── benchmarks.py
│   └── models/
│       ├── __init__.py
│       ├── base.py
│       ├── debt.py
│       ├── equity.py
│       ├── derivative.py
│       └── mapper.py
├── examples/
├── tools/
├── docs/
│   └── rts_23_financial_instrument_reference_data_schema.md
├── README.md
├── CHANGELOG.md
└── setup.py
```

## Requirements

- Python >= 3.8
- pandas >= 1.3.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- lxml >= 4.6.0
- tqdm >= 4.60.0

## Documentation

For detailed information on ESMA datasets:
- [ESMA Registers and Data](https://www.esma.europa.eu/database-library/registers-and-data)
- [MiFID II Transparency](https://www.esma.europa.eu/policy-rules/mifid-ii-and-mifir)
- [RTS 23 Schema Reference](docs/rts_23_financial_instrument_reference_data_schema.md)

## License

This project is licensed under the MIT License.

## Disclaimer

This package is not affiliated with or endorsed by ESMA. It is an independent tool for accessing publicly available ESMA data.



