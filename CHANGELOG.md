# Changelog

All notable changes to the esma-dm project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

#### CFI Classification System (2025-01-03)
- Complete ISO 10962 CFI (Classification of Financial Instruments) implementation
- Full decoding of all CFI categories: E, D, C, F, O, S, H, R, I, J, K, L, T, M
- Comprehensive attribute decoders for each category and group
- CFI dataclass with validation and description methods
- CFIInstrumentTypeManager for FIRDS/FITRS file mapping
- Integration into storage layer for automatic classification
- New methods: classify_instrument(), get_instruments_by_cfi_category()
- Enhanced search_instruments() with CFI descriptions
- Example script demonstrating CFI classification (03_cfi_classification.py)

### Changed

#### ISO 10962 Compliance - Asset Type Mapping (2025-01-03)
- Corrected FIRDS file type mappings to match ISO 10962 CFI standard
- I type: Renamed from "Indices" to "Spot" (spot contracts and indices)
- J type: Renamed from "Listed Options" to "Forwards" (forward contracts and warrants)
- Table names: index_instruments → spot_instruments, listed_option_instruments → forward_instruments
- Schema functions: create_index_table → create_spot_table, create_listed_option_table → create_forward_table
- Bulk insert methods: insert_indices → insert_spots, insert_listed_options → insert_forwards
- Updated documentation and routing logic to align with CFI Category enum

#### Complete Asset Type Support (2025-01-03)
- Schema support for all 10 FIRDS asset types (C, D, E, F, H, I, J, O, R, S)
- Dedicated tables: futures_instruments, option_instruments, swap_instruments, forward_instruments, rights_instruments, civ_instruments, spot_instruments
- Asset-specific field mappings for each instrument type
- Comprehensive field extraction for derivatives (options, swaps, futures, forwards)
- Commodity, FX, and interest rate product attributes
- Schema definitions separated into schema.py module (286 lines)
- Bulk insert handlers refactored into bulk_inserters.py module (543 lines)
- Comprehensive testing with 2.37M real FIRDS instruments across all 10 types
- Complete database schema documentation in database_schema.txt

### Changed

#### Storage Architecture (2025-01-03)
- Expanded from 5 tables to 11 tables for complete asset type coverage
- Enhanced debt instruments schema with floating rate fields
- Enhanced futures schema with commodity product classifications
- Options schema supports strike price variations (monetary, percentage, basis points)
- Swaps schema includes interest rate and FX swap attributes
- Listed options schema for exchange-traded options with commodity attributes
- Rights/entitlements schema for warrants and subscription rights
- CIV schema for collective investment vehicles
- Index schema for commodity and other indices
- Modular architecture: duckdb_store.py (307 lines) imports schema and bulk_inserters modules
- Reduced main storage class from 1,139 lines to 307 lines (73% reduction)

#### Vectorized Storage Backend (2025-01-03)
- DuckDBVectorizedStorage class with star schema architecture
- Master instruments table with core fields (ISIN, CFI, type, issuer, name, currency)
- Asset-specific tables: equity_instruments, debt_instruments, derivative_instruments, other_instruments
- Vectorized bulk loading using pandas groupby and DuckDB bulk insert
- Performance: 35,000-81,000 instruments/second (640x improvement over row-by-row processing)
- Indexed columns for fast queries on instrument_type, cfi_code, and trading_venue_id
- Foreign key relationships between master and detail tables

### Changed

#### Storage Architecture (2026-01-03)
- Replaced row-by-row mapper processing with vectorized CSV loading
- Eliminated JSON blob storage in favor of normalized relational schema
- Each asset type has dedicated table with type-specific fields
- Query pattern: lookup master table by ISIN, join to specific table by type

### Performance

#### Benchmark Results (2025-01-03)
- Tested with all 10 asset types:
  - C: 127K instruments in 0.89s (142,285 inst/sec)
  - D: 500K instruments in 8.18s (61,149 inst/sec)
  - E: 500K instruments in 6.02s (83,100 inst/sec)
  - F: 48K instruments in 1.53s (31,368 inst/sec)
  - H: 500K instruments in 13.39s (37,342 inst/sec)
  - I: 3 instruments in 0.06s (51 inst/sec)
  - J: 117K instruments in 3.14s (37,353 inst/sec)
  - O: 500K instruments in 13.02s (38,403 inst/sec)
  - R: 500K instruments in 11.25s (44,464 inst/sec)
  - S: 500K instruments in 13.60s (36,752 inst/sec)
- **Total: 2.37M instruments in 71.08s (33,374 inst/sec)**
- Database size: 625.8 MB
- All asset-specific fields correctly extracted and stored

#### Index Manager (2026-01-02)
- IndexManager class for fast ISIN lookups using lightweight JSON index
- Automatic index creation and maintenance
- `rebuild_index()` method to manually rebuild index
- `get_index_stats()` method to view index statistics
- Index file stored as `_isin_index.json` in cache directory
- No database dependencies - simple JSON mapping of ISINs to file locations

#### FIRDS Enhancements (2026-01-02)
- Enhanced `reference()` method to use index for fast lookups
- Automatic index rebuild if ISIN not found
- `use_cache` parameter in `reference()` to control cache usage
- Efficient row-level reading from CSV files using index

## [0.1.0] - 2024-12-31

### Added

#### Core Package
- Initial package structure with modular client architecture
- FIRDSClient for Financial Instruments Reference Data System
- FITRSClient for Financial Instruments Transparency System
- SSRClient for Short Selling Regulation data
- BenchmarksClient stub for future implementation
- Config class for configuration management with environment variable support
- Utils module with XML parsing, file download, and caching utilities
- Project-level downloads folder at `./downloads/data`

#### FIRDS Enhancements (2024-12-31)
- FIRDSFile dataclass for structured file metadata with automatic filename parsing
- FileType enum (FULINS, DLTINS)
- Enhanced AssetType enum with descriptions (10 types: C, D, E, F, H, I, J, O, R, S)
- CommodityBaseProduct enum (12 classifications: AGRI, NRGY, METL, EMIS, etc.)
- OptionType enum (CALL, PUT, OTHR)
- ExerciseStyle enum (EURO, AMER, BRMN, ASIA)
- DeliveryType enum (PHYS, CASH, OPTL)
- BondSeniority enum (SNDB, MZZD, SBOD, JUND)
- `reference()` method for single ISIN lookup with automatic validation and cached file search
- `get_files_metadata()` method returning structured FIRDSFile objects
- `get_delta_files()` method for tracking incremental changes
- Enhanced `get_file_list()` with file_type and asset_type filters
- `validate_isin()` static method for ISO 6166 validation
- `validate_lei()` static method for ISO 17442 validation
- `validate_cfi()` static method for ISO 10962 validation

#### Data Models (2024-12-31)
- Normalized reference data models for all asset types
- RecordType enum (NEW, UPDATE, DELETE)
- TradingVenueAttributes dataclass (6 fields)
- TechnicalAttributes dataclass (4 fields)
- Instrument base class (12 fields) with type checking methods
- DebtInstrument class (24 total fields: 12 base + 12 debt-specific)
- EquityInstrument class (17 total fields: 12 base + 5 equity-specific)
- OptionAttributes dataclass (5 fields)
- FutureAttributes dataclass (3 fields)
- DerivativeInstrument class (27 total fields: 12 base + 15 derivative-specific)
- InstrumentMapper class for converting raw FIRDS data to typed models
- Schema introspection via `get_schema()` methods on all model classes

#### Bug Fixes (2024-12-31)
- Fixed XML parsing bug in FULINS files (previously only parsing 1 record instead of full dataset)
- Fixed XML parsing bug in DLTINS files (previously only parsing 1 record instead of 500,000+ records)
- Fixed type error in mapper.py where `issuer_request` was parsed as string instead of boolean
- Added `_parse_bool()` method to handle boolean conversion from XML string values
- Fixed type error in mapper.py where `never_published` was using string comparison instead of boolean parsing

#### Performance Improvements (2024-12-31)
- Implemented date-based filtering reducing file queries from 10,000 to ~200 files
- Moved cache from user home directory to project `./downloads` folder
- Optimized XML parsing with lxml for large files
- Batch operations support for multiple ISINs

#### Documentation (2024-12-31)
- Professional README.md describing package and API usage
- CHANGELOG.md with timestamped changes
- GitHub Copilot instructions at `.github/copilot-instructions.md`
- RTS 23 schema reference at `docs/rts_23_financial_instrument_reference_data_schema.md`

#### Examples and Tools
- `examples/firds_usage.py` - Basic FIRDS usage
- `examples/fitrs_usage.py` - Basic FITRS usage
- `examples/ssr_usage.py` - Basic SSR usage
- `examples/firds_normalized_data.py` - Normalized data models usage
- `examples/firds_advanced_usage.py` - Advanced FIRDS features (enums, validation, delta files)
- `tools/inspect_firds_files.py` - FIRDS file inspection utility
- `tools/display_schemas.py` - Schema display tool (outputs to docs/SCHEMA_REFERENCE.txt)
- `tools/analyze_schema_coverage.py` - Schema coverage analysis tool
- `tools/test_firds_enhancements.py` - FIRDS enhancements test suite

### Changed
- Cache location moved from user home to project `./downloads/data` directory
- AssetType enum enhanced with full descriptions
- File filtering now supports both file_type and asset_type parameters
- All datetime fields properly parsed to Python datetime objects
- Boolean fields properly parsed to Python bool type

### Technical Details

#### Standards Compliance
- ISO 6166 (ISIN) validation
- ISO 17442 (LEI) validation
- ISO 10962 (CFI) validation
- RTS 23 (MiFIR) specifications
- ISO 8601 date/time formats

#### Architecture
- Modular client design with separate modules per dataset
- Dataclasses for structured data representation
- Enums for type-safe classifications
- Static validation methods for data quality
- Schema introspection for runtime type information
- Mapper pattern for data normalization

#### Asset Type Coverage
All 10 CFI asset types supported and tested:
- C: Collective Investment Vehicles
- D: Debt Instruments (bonds, notes)
- E: Equities (shares, units)
- F: Futures
- H: Rights, Warrants
- I: Options
- J: Strategies, Multi-leg
- O: Others (miscellaneous)
- R: Referential Instruments
- S: Swaps

### Dependencies
- Python >= 3.8
- pandas >= 1.3.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- lxml >= 4.6.0
- tqdm >= 4.60.0

### Testing
- All FIRDS enhancements verified with unit tests
- Package successfully tested with real ESMA data
- Validated with 10,000+ instrument records across all asset types
- Delta file parsing tested with 500,000+ records

### Known Limitations
- BenchmarksClient is a stub implementation (to be completed)
- FITRS DVCAP historical data not yet implemented
- No automated testing framework yet (manual testing only)

### Security
- No credentials or API keys required (public ESMA data)
- All data accessed via HTTPS
- No sensitive data stored in cache
