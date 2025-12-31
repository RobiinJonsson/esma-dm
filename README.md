# ESMA Data Manager (esma-dm)

A comprehensive Python package for accessing ESMA (European Securities and Markets Authority) published reference data and transparency information.

## Features

🎯 **Modular Design** - Separate clients for different ESMA datasets
- **FIRDS** - Financial Instruments Reference Data System
- **FITRS** - Financial Instruments Transparency System
- **SSR** - Short Selling Regulation exempted shares
- **Benchmarks** - Benchmark data (coming soon)

📦 **Easy to Use** - Simple, intuitive API
```python
from esma_dm import FIRDSClient, FITRSClient

# Get equity reference data
firds = FIRDSClient()
equities = firds.get_latest_full_files(asset_type='E')

# Get transparency data
fitrs = FITRSClient()
transparency = fitrs.get_latest_full_files(asset_type='E')
```

⚡ **Performance Optimized**
- Built-in caching mechanism
- Batch operations support
- Efficient XML parsing

🔧 **Configurable**
- Custom download directories
- Logging levels
- Cache control

## Installation

```bash
pip install esma-dm
```

### Development Installation

```bash
git clone https://github.com/yourusername/esma-dm.git
cd esma-dm
pip install -e ".[dev]"
```

## Quick Start

### FIRDS - Reference Data

```python
from esma_dm import FIRDSClient

# Initialize client
firds = FIRDSClient(date_from='2024-01-01')

# Get all available files
files = firds.get_file_list()
print(files[['file_name', 'publication_date']])

# Get latest equity instruments
equities = firds.get_latest_full_files(asset_type='E')
print(f"Retrieved {len(equities)} equity instruments")

# Get specific ISINs
instruments = firds.get_instruments([
    'GB00B1YW4409',  # Sage Group
    'US0378331005',  # Apple Inc
])
```

### FITRS - Transparency Data

```python
from esma_dm import FITRSClient

# Initialize client
fitrs = FITRSClient(date_from='2024-01-01')

# Get equity transparency data
eq_transparency = fitrs.get_latest_full_files(
    asset_type='E',
    instrument_type='equity'
)

# Get DVCAP (Double Volume Cap) data
dvcap = fitrs.get_dvcap_latest()

# Get transparency for specific ISINs
my_transparency = fitrs.get_instruments([
    'GB00B1YW4409',
    'US0378331005'
])
```

### SSR - Short Selling Regulation

```python
from esma_dm import SSRClient

# Initialize client
ssr = SSRClient()

# Get currently active exemptions
active_exemptions = ssr.get_exempted_shares(today_only=True)

# Get exemptions for specific country
uk_exemptions = ssr.get_exempted_shares_by_country('GB')

# Get all exemptions (including expired)
all_exemptions = ssr.get_exempted_shares(today_only=False)
```

## Configuration

### Using Default Configuration

```python
from esma_dm import FIRDSClient

# Uses default configuration (~/.esma_dm/data)
firds = FIRDSClient()
```

### Custom Configuration

```python
from esma_dm import FIRDSClient, Config
from pathlib import Path

# Create custom configuration
config = Config(
    downloads_path=Path("/custom/path/to/data"),
    cache_enabled=True,
    log_level="DEBUG"
)

# Use custom configuration
firds = FIRDSClient(config=config)
```

### Environment Variables

```bash
export ESMA_DM_DOWNLOADS_PATH="/custom/path/to/data"
export ESMA_DM_CACHE_ENABLED="true"
export ESMA_DM_LOG_LEVEL="INFO"
```

```python
from esma_dm import Config, FIRDSClient

# Load configuration from environment
config = Config.from_env()
firds = FIRDSClient(config=config)
```

## Asset Types

ESMA uses CFI (Classification of Financial Instruments) codes. The first character represents the asset type:

| Code | Asset Type | Description |
|------|------------|-------------|
| C | Collective Investment | Investment funds, ETFs |
| D | Debt | Bonds, notes, treasury bills |
| E | Equity | Shares, stocks |
| F | Futures | Future contracts |
| H | Rights | Subscription rights |
| I | Options | Options contracts |
| J | Strategies | Structured products |
| O | Others | Miscellaneous |
| R | Referential | Reference instruments |
| S | Swaps | Swap contracts |

## Advanced Usage

### Batch Processing

```python
from esma_dm import FIRDSClient

firds = FIRDSClient()

# Get consolidated data for asset type
consolidated = firds.get_batch_consolidated_data(asset_type='E')

# Process multiple ISINs efficiently
isin_list = ['GB00B1YW4409', 'US0378331005', 'DE0005140008']
instruments = firds.get_instruments(isin_list)
```

### Force Update (Skip Cache)

```python
from esma_dm import FIRDSClient

firds = FIRDSClient()

# Force re-download and update cache
fresh_data = firds.get_latest_full_files(
    asset_type='E',
    update=True  # Skip cache, download fresh
)
```

### Download Specific Files

```python
from esma_dm import FIRDSClient

firds = FIRDSClient()

# Get file list
files = firds.get_file_list()

# Download specific file by URL
specific_file = files.iloc[0]
df = firds.download_file(specific_file['download_link'])
```

## Data Structure

### FIRDS Reference Data

Typical columns include:
- `Id` - ISIN identifier
- `FullNm` - Full name of instrument
- `ClssfctnTp` - Classification type (CFI code)
- `CmmdtyDerivInd` - Commodity derivative indicator
- `NtnlCcy` - Notional currency
- `TradgVnRltdAttrbts_*` - Trading venue attributes

### FITRS Transparency Data

Typical columns include:
- `Id` - ISIN identifier
- `TradgVn` - Trading venue
- `AvrgDalyNbOfTxs` - Average daily number of transactions
- `AvrgDalyTrnvr` - Average daily turnover
- `AvrgTxVal` - Average transaction value

## Documentation

For detailed documentation on ESMA datasets, see:
- [FIRDS Instructions](https://www.esma.europa.eu/database-library/registers-and-data)
- [MiFID II Transparency](https://www.esma.europa.eu/policy-rules/mifid-ii-and-mifir)

## Project Structure

```
esma-dm/
├── esma_dm/
│   ├── __init__.py       # Package initialization
│   ├── config.py         # Configuration management
│   ├── utils.py          # Shared utilities
│   ├── firds.py          # FIRDS client
│   ├── fitrs.py          # FITRS client
│   ├── ssr.py            # SSR client
│   └── benchmarks.py     # Benchmarks client (stub)
├── docs/                 # Documentation PDFs
├── examples/             # Usage examples
├── tests/                # Unit tests
├── setup.py              # Package setup
├── README.md             # This file
└── requirements.txt      # Dependencies
```

## Requirements

- Python >= 3.8
- pandas >= 1.3.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- lxml >= 4.6.0
- tqdm >= 4.60.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This package is not affiliated with or endorsed by ESMA. It is an independent tool for accessing publicly available ESMA data.

## Support

For issues and questions:
- GitHub Issues: https://github.com/yourusername/esma-dm/issues
- Documentation: https://github.com/yourusername/esma-dm#readme

## Changelog

### 0.1.0 (2025-01-XX)
- Initial release
- FIRDS client implementation
- FITRS client implementation
- SSR client implementation
- Benchmarks client stub
- Configuration management
- Caching system
- XML parsing utilities
