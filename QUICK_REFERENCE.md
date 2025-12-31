# ESMA Data Manager - Quick Reference

## Installation

```bash
pip install esma-dm
```

## Quick Start

```python
from esma_dm import FIRDSClient, FITRSClient, SSRClient

# FIRDS - Reference Data
firds = FIRDSClient()
equities = firds.get_latest_full_files(asset_type='E')

# FITRS - Transparency
fitrs = FITRSClient()
transparency = fitrs.get_latest_full_files(asset_type='E', instrument_type='equity')

# SSR - Short Selling
ssr = SSRClient()
exemptions = ssr.get_exempted_shares(today_only=True)
```

## Asset Types (CFI Codes)

| Code | Type | Example |
|------|------|---------|
| E | Equity | Shares, stocks |
| D | Debt | Bonds, notes |
| C | Collective Investment | ETFs, funds |
| F | Futures | Future contracts |
| I | Options | Option contracts |
| S | Swaps | Swap contracts |

## Common Operations

### Get File Lists
```python
# FIRDS files
firds = FIRDSClient()
files = firds.get_file_list()

# FITRS files
fitrs = FITRSClient()
files = fitrs.get_file_list()
```

### Get Latest Full Files
```python
firds = FIRDSClient()

# All equities
equities = firds.get_latest_full_files(asset_type='E')

# All debt instruments
debt = firds.get_latest_full_files(asset_type='D')

# With specific ISINs
filtered = firds.get_latest_full_files(
    asset_type='E',
    isin_filter=['GB00B1YW4409']
)
```

### Get Specific Instruments
```python
firds = FIRDSClient()

# Single asset type
instruments = firds.get_instruments(
    ['GB00B1YW4409', 'US0378331005'],
    asset_type='E'
)

# Search all asset types
instruments = firds.get_instruments(['GB00B1YW4409'])
```

### Get Transparency Data
```python
fitrs = FITRSClient()

# Equity transparency
eq_trans = fitrs.get_latest_full_files(
    asset_type='E',
    instrument_type='equity'
)

# Non-equity transparency
non_eq_trans = fitrs.get_latest_full_files(
    asset_type='D',
    instrument_type='non_equity'
)

# DVCAP data
dvcap = fitrs.get_dvcap_latest()
```

### SSR Exemptions
```python
ssr = SSRClient()

# Current exemptions
current = ssr.get_exempted_shares(today_only=True)

# All exemptions
all_exemptions = ssr.get_exempted_shares(today_only=False)

# Specific country
uk = ssr.get_exempted_shares_by_country('GB')
```

### Force Update (Skip Cache)
```python
firds = FIRDSClient()

# Force re-download
fresh = firds.get_latest_full_files(asset_type='E', update=True)
```

## Configuration

### Default Configuration
```python
from esma_dm import FIRDSClient

# Uses ~/.esma_dm/data
firds = FIRDSClient()
```

### Custom Configuration
```python
from esma_dm import Config, FIRDSClient
from pathlib import Path

config = Config(
    downloads_path=Path("/custom/data"),
    cache_enabled=True,
    log_level="DEBUG"
)

firds = FIRDSClient(config=config)
```

### Environment Variables
```bash
export ESMA_DM_DOWNLOADS_PATH="/custom/data"
export ESMA_DM_CACHE_ENABLED="true"
export ESMA_DM_LOG_LEVEL="INFO"
```

```python
from esma_dm import Config, FIRDSClient

config = Config.from_env()
firds = FIRDSClient(config=config)
```

## Advanced Usage

### Combine Multiple Datasets
```python
from esma_dm import FIRDSClient, FITRSClient
import pandas as pd

firds = FIRDSClient()
fitrs = FITRSClient()

# Get both reference and transparency
ref_data = firds.get_instruments(['GB00B1YW4409'])
trans_data = fitrs.get_instruments(['GB00B1YW4409'])

# Merge on ISIN
combined = pd.merge(ref_data, trans_data, on='Id', how='outer')
```

### Batch Processing
```python
firds = FIRDSClient()

# Get consolidated data
consolidated = firds.get_batch_consolidated_data(asset_type='E')

# Process multiple ISINs
isin_list = ['GB00B1YW4409', 'US0378331005', 'DE0005140008']
results = firds.get_instruments(isin_list)
```

### Download Specific Files
```python
firds = FIRDSClient()

# Get file list
files = firds.get_file_list()

# Download specific file
url = files.iloc[0]['download_link']
data = firds.download_file(url)
```

## Typical Column Names

### FIRDS Columns
- `Id` - ISIN
- `FullNm` - Full name
- `ClssfctnTp` - CFI code
- `CmmdtyDerivInd` - Commodity derivative indicator
- `NtnlCcy` - Notional currency

### FITRS Columns
- `Id` - ISIN
- `TradgVn` - Trading venue
- `AvrgDalyNbOfTxs` - Avg daily transactions
- `AvrgDalyTrnvr` - Avg daily turnover
- `AvrgTxVal` - Avg transaction value

### SSR Columns
- `shs_isin` - ISIN
- `shs_countryCode` - Country code
- `shs_exemptionStartDate` - Start date
- `shs_modificationBDate` - Modification before date

## Error Handling

```python
from esma_dm import FIRDSClient

firds = FIRDSClient()

try:
    data = firds.get_latest_full_files(asset_type='E')
except ValueError as e:
    print(f"Invalid parameter: {e}")
except Exception as e:
    print(f"Error: {e}")
```

## Logging

```python
import logging
from esma_dm import FIRDSClient, Config

# Set log level via config
config = Config(log_level="DEBUG")
firds = FIRDSClient(config=config)

# Or configure logging directly
logging.basicConfig(level=logging.DEBUG)
```

## Countries (SSR)

AT, BE, BG, CY, CZ, DE, DK, EE, ES, FI, FR, GR, HR, HU, IE, IT, LT, LU, LV, MT, NL, PL, PT, RO, SE, SI, SK, NO, GB

## Links

- **ESMA Website**: https://registers.esma.europa.eu/publication/
- **Documentation**: [README.md](README.md)
- **Migration Guide**: [MIGRATION.md](MIGRATION.md)
- **Examples**: [examples/](examples/)
