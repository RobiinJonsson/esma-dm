# ESMA Data Manager CLI

Command-line interface for managing ESMA financial data.

## Quick Reference

### File Management

```bash
# List files from ESMA
esma-dm firds list [--type FULINS|DLTINS] [--asset C|D|E|F|H|I|J|O|R|S] [--date-from DATE] [--date-to DATE]

# Download files
esma-dm firds download --asset <TYPE> [--type FULINS|DLTINS] [--update]

# List cached files
esma-dm firds cache [--asset TYPE] [--type FULINS|DLTINS]

# Inspect file fields
esma-dm firds fields <filename>

# Preview file contents
esma-dm firds head <filename> [--rows N] [--columns "col1,col2"]

# Show available types
esma-dm firds types

# Show cache statistics
esma-dm firds stats
```

## Command Groups

### FIRDS (`esma-dm firds`)

Comprehensive file management for ESMA FIRDS reference data using the unified file_manager module.

#### list
List available files from ESMA FIRDS register with automatic pagination.

Options:
- `--type`: Filter by file type (FULINS, DLTINS)
- `--asset`: Filter by asset type (C, D, E, F, H, I, J, O, R, S)
- `--date-from`: Start date (YYYY-MM-DD)
- `--date-to`: End date (YYYY-MM-DD)
- `--limit`: Maximum number of files (omit for all files)
- `--fetch-all`: Explicitly enable pagination

**Features:**
- Automatic pagination (fetches all 796+ equity files)
- Date range filtering
- File metadata extraction (type, asset, date, parts)

#### download
Download latest FULINS files for a specific asset type.

Options:
- `--asset`: **Required** - Asset type to download
- `--type`: File type (default: FULINS)
- `--update/--no-update`: Force fresh download (default: False)

**Features:**
- Intelligent caching (skips if already cached)
- Progress indicators
- Size reporting

#### cache
List files in local cache directory with filtering and statistics.

Options:
- `--asset`: Filter by asset type
- `--type`: Filter by file type

**Features:**
- File counts by type and asset
- Total size calculation
- Modification dates

#### fields
List all field names (columns) in a CSV file.

Arguments:
- `file_path`: Path to CSV file (relative to cache or absolute)

#### head
Display the first N rows of a CSV file.

Arguments:
- `file_path`: Path to CSV file

Options:
- `--rows, -n`: Number of rows (default: 10)
- `--columns, -c`: Comma-separated column names

#### types **NEW**
List available file types and asset types with descriptions.

Shows:
- File types (FULINS, DLTINS, FULCAN)
- Asset types (C-S with full descriptions)
- Usage guidance

#### stats **NEW**
Show comprehensive cache statistics.

Displays:
- Total file count
- Total size in MB
- Breakdown by file type
- Breakdown by asset type

## Examples

### Workflow: Explore and Download

```bash
# 1. See available types
esma-dm firds types

# 2. Check what's available
esma-dm firds list --type FULINS --asset E

# 3. Download latest equity files
esma-dm firds download --asset E

# 4. Check what was downloaded
esma-dm firds cache --asset E

# 5. View statistics
esma-dm firds stats

# 6. Inspect file structure
esma-dm firds fields FULINS_E_20260207_01of02_data.csv

# 7. Preview the data
esma-dm firds head FULINS_E_20260207_01of02_data.csv -n 5
```

### Working with Multiple Asset Types

```bash
# Download debt instruments
esma-dm firds download --asset D

# Download swaps
esma-dm firds download --asset S

# List all cached files by type
esma-dm firds cache --type FULINS
```

### Filtering and Analysis

```bash
# Preview specific columns
esma-dm firds head FULINS_E_20260117_01of02_data.csv \
  --columns "Id,RefData_FinInstrmGnlAttrbts_FullNm,RefData_FinInstrmGnlAttrbts_ClssfctnTp" \
  --rows 20
```

## Asset Types

- **C**: Collective Investment Vehicles
- **D**: Debt Instruments
- **E**: Equities
- **F**: Futures
- **H**: Swaps (other)
- **I**: Indexes
- **J**: Forwards
- **O**: Options
- **R**: Rates (Interest Rate Derivatives)
- **S**: Swaps

## File Types

- **FULINS**: Full instrument reference data snapshots
- **DLTINS**: Delta files (daily changes: NEW, MODIFIED, TERMINATED, CANCELLED)

## Tips

1. **Cache Usage**: Downloaded files are cached by default. Use `--update` to force fresh downloads.

2. **Path Resolution**: File commands accept both relative names (searches cache) and absolute paths.

3. **Rich Output**: All commands use rich terminal formatting. Best viewed in modern terminals with color support.

4. **Help**: Use `--help` with any command for detailed information:
   ```bash
   esma-dm firds list --help
   ```

5. **Combining with Python**: Use CLI for exploration, then Python API for analysis:
   ```bash
   # Download with CLI
   esma-dm firds download --asset E
   
   # Analyze with Python
   python
   >>> import esma_dm as edm
   >>> firds = edm.FIRDSClient()
   >>> firds.index_cached_files()
   ```

## Architecture

The CLI is built with:
- **click**: Command-line interface framework
- **rich**: Terminal formatting and tables
- Clean separation from core library code
- Modular command groups for extensibility

## Future Commands

Planned command groups:
- `esma-dm database`: Database management (init, drop, stats)
- `esma-dm query`: Interactive SQL queries
- `esma-dm analyze`: Data analysis and reports
