# Migration Guide

## Migrating from Old Code to esma-dm Package

This guide helps you migrate from the old `esma_data_loader.py` / `esma_utils.py` structure to the new modular `esma-dm` package.

## Overview of Changes

### Old Structure
```
esma_data_loader.py  # Single class for everything
esma_utils.py        # All utilities mixed together
```

### New Structure
```
esma_dm/
├── __init__.py       # Package entry point
├── config.py         # Configuration management
├── utils.py          # Shared utilities
├── firds.py          # FIRDS-specific client
├── fitrs.py          # FITRS-specific client
├── ssr.py            # SSR-specific client
└── benchmarks.py     # Benchmarks (future)
```

## Key Changes

### 1. Imports

**Old:**
```python
from esma_data_loader import EsmaDataLoader
```

**New:**
```python
from esma_dm import FIRDSClient, FITRSClient, SSRClient
```

### 2. Client Initialization

**Old:**
```python
loader = EsmaDataLoader(
    creation_date_from='2024-01-01',
    creation_date_to='2024-12-31',
    limit='10000'
)
```

**New:**
```python
# Separate clients for different datasets
firds = FIRDSClient(date_from='2024-01-01', date_to='2024-12-31')
fitrs = FITRSClient(date_from='2024-01-01', date_to='2024-12-31')
ssr = SSRClient()
```

### 3. Getting File Lists

**Old:**
```python
# Mixed FIRDS, FITRS, DVCAP in one method
files = loader.load_mifid_file_list(datasets=['firds', 'fitrs', 'dvcap'])
```

**New:**
```python
# Separate methods for each dataset
firds_files = firds.get_file_list()
fitrs_files = fitrs.get_file_list()
dvcap_data = fitrs.get_dvcap_latest()
```

### 4. Loading Latest Files

**Old:**
```python
data = loader.load_latest_files(
    file_type='Full',
    vcap=False,
    isin=['GB00B1YW4409'],
    cfi='E',
    eqt=True,
    update=False
)
```

**New:**
```python
# Cleaner API with asset_type instead of cfi
data = firds.get_latest_full_files(
    asset_type='E',
    isin_filter=['GB00B1YW4409'],
    update=False
)
```

### 5. SSR Exempted Shares

**Old:**
```python
loader = EsmaDataLoader()
exempted = loader.load_ssr_exempted_shares(today=True)
```

**New:**
```python
ssr = SSRClient()
exempted = ssr.get_exempted_shares(today_only=True)
```

### 6. FCA FIRDS

**Old:**
```python
loader = EsmaDataLoader()
fca_data = loader.load_fca_firds_file_list()
```

**New:**
```python
# FCA FIRDS integrated into main FIRDS client
firds = FIRDSClient()
files = firds.get_file_list()
# Filter for FCA files if needed
```

### 7. Configuration

**Old:**
```python
# Configuration was hardcoded or in external config
from marketdata_api.config import esmaConfig
```

**New:**
```python
from esma_dm import Config

# Use default config
config = Config()

# Or customize
config = Config(
    downloads_path=Path("/custom/path"),
    cache_enabled=True,
    log_level="DEBUG"
)

# Use with clients
firds = FIRDSClient(config=config)
```

## Migration Examples

### Example 1: Get Latest Equity Reference Data

**Old Code:**
```python
from esma_data_loader import EsmaDataLoader

loader = EsmaDataLoader(creation_date_from='2024-01-01')
equities = loader.load_latest_files(
    file_type='Full',
    cfi='E',
    eqt=True
)
```

**New Code:**
```python
from esma_dm import FIRDSClient

firds = FIRDSClient(date_from='2024-01-01')
equities = firds.get_latest_full_files(asset_type='E')
```

### Example 2: Get Specific ISINs

**Old Code:**
```python
loader = EsmaDataLoader()
instruments = loader.load_latest_files(
    cfi='E',
    isin=['GB00B1YW4409', 'US0378331005']
)
```

**New Code:**
```python
from esma_dm import FIRDSClient

firds = FIRDSClient()
instruments = firds.get_instruments([
    'GB00B1YW4409',
    'US0378331005'
])
```

### Example 3: SSR Exemptions

**Old Code:**
```python
loader = EsmaDataLoader()
exemptions = loader.load_ssr_exempted_shares(today=True)
```

**New Code:**
```python
from esma_dm import SSRClient

ssr = SSRClient()
exemptions = ssr.get_exempted_shares(today_only=True)
```

### Example 4: Download Specific File

**Old Code:**
```python
loader = EsmaDataLoader()
data = loader.download_file(url='https://...', update=True)
```

**New Code:**
```python
from esma_dm import FIRDSClient

firds = FIRDSClient()
data = firds.download_file(url='https://...', update=True)
```

## Parameter Mapping

| Old Parameter | New Parameter | Notes |
|--------------|---------------|-------|
| `creation_date_from` | `date_from` | Simplified name |
| `creation_date_to` | `date_to` | Simplified name |
| `cfi` | `asset_type` | Same values (C, D, E, etc.) |
| `eqt` | `instrument_type` | Now uses 'equity' or 'non_equity' |
| `vcap` | N/A | Use `fitrs.get_dvcap_latest()` |
| `isin` | `isin_filter` or `isin_list` | More explicit naming |
| `today` | `today_only` | More explicit naming |

## Benefits of New Structure

1. **Modularity**: Each dataset has its own client
2. **Type Safety**: Better type hints and validation
3. **Configuration**: Centralized, flexible configuration
4. **Caching**: Improved caching mechanism
5. **Error Handling**: Better error messages and handling
6. **Documentation**: Comprehensive docstrings and examples
7. **Testing**: Unit tests for all components
8. **Extensibility**: Easy to add new datasets

## Gradual Migration Strategy

You can migrate gradually by:

1. **Install the package** alongside old code
2. **Start using new clients** for new features
3. **Gradually replace** old calls with new ones
4. **Keep old files** as backup during transition
5. **Remove old code** once fully migrated

## Troubleshooting

### Issue: Import errors

**Solution:** Ensure package is installed:
```bash
pip install -e .
```

### Issue: Different data structure

**Solution:** The new package returns pandas DataFrames with cleaned column names. You may need to adjust column references in downstream code.

### Issue: Cache location

**Solution:** New package uses `~/.esma_dm/data` by default. To use old cache location:
```python
config = Config(downloads_path=Path("/old/cache/path"))
firds = FIRDSClient(config=config)
```

## Need Help?

- Check the [README.md](README.md) for detailed documentation
- Review [examples/](examples/) for usage patterns
- Open an issue on GitHub for questions
