# Changelog

All notable changes to the esma-dm project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.6] - 2026-03-08

### Added - Database Management CLI (`esma-dm db`)

New `db` command group with three subcommands:

`esma-dm db stats [--tables]`
- Database file path and size
- Total instruments, total listings, distinct trading venues
- Instrument count per asset type with detail table name
- FITRS transparency table row counts (when populated)
- `--tables` flag enumerates all 24 tables with row counts

`esma-dm db reinit [--mode current|history] [--yes]`
- Drops the database file and reinitializes the schema
- Interactive confirmation prompt (bypass with `--yes`)
- Reports new file size after fresh schema creation

`esma-dm db drop [--mode current|history] [--yes]`
- Removes the database file without reinitializing
- Interactive confirmation prompt (bypass with `--yes`)

### Changed

- `firds` CLI group description updated from "file management commands" to
  "FIRDS reference data — file management and instrument lookup" to reflect
  the addition of `reference` and `search` subcommands.
- `esma_dm/cli/main.py` bumped version to reflect new command group.

---

## [0.3.5] - 2026-03-08

### Added - Instrument Lookup and Search CLI Commands

Two new `firds` CLI commands for querying the FIRDS database directly from the terminal:

`esma-dm firds reference <ISIN>`
- Displays master fields (ISIN, CFI code, full name, short name, issuer, source file, instrument type)
- Renders full CFI classification with human-readable attribute labels (e.g. Voting Right, Payment Status)
- Shows asset-specific detail columns from the relevant typed table (e.g. `expiry_date`, `strike_price`, `underlying_instrument` for swaps and options)
- Supports `--mode current|history`

`esma-dm firds search <query>`
- Case-insensitive match against instrument name and ISIN prefix
- Supports `--asset` filter and `--limit`
- Returns tabular results with ISIN, type, CFI code, name, and currency
- Supports `--mode current|history`

### Fixed - `get_instrument` column mapping in `storage/duckdb/queries.py`

- `get_instrument()` was hardcoding positional indices for the result row. `BASE_FIELDS_CURRENT` includes `short_name` between `full_name` and `issuer`, so every field from `issuer` onward was misassigned (notably `instrument_type` was showing the `indexed_at` timestamp value).
- Replaced positional mapping with cursor `.description`-based column mapping — fully robust to future schema changes.
- Replaced non-existent `CFI.from_code()` call with `decode_cfi()`, which also returns all four CFI attribute labels with their full descriptions instead of generic `attribute1`/`attribute2` keys.

---

## [0.3.4] - 2026-03-07

### Added - Complete FIRDS Instrument Model Layer (All 14 CFI Categories)

Completed the model layer so every ISO 10962 CFI category has a dedicated Python
dataclass. Previously only `EquityInstrument` (E*) and `DebtInstrument` (D*) had
dedicated models; all other categories fell back to the generic `DerivativeInstrument`
or base `Instrument`.

New model files (one per CFI category):
- `esma_dm/models/swap.py` — `SwapInstrument` (S*): IR, equity, FX, CDS, TRS
- `esma_dm/models/futures.py` — `FutureInstrument` (F*): exchange-traded futures
- `esma_dm/models/listed_option.py` — `ListedOptionInstrument` (O*): standardised options
- `esma_dm/models/non_standard.py` — `NonStandardDerivativeInstrument` (H*): warrants, OTC options, swaptions
- `esma_dm/models/forward.py` — `ForwardInstrument` (J*): FX forwards, FRAs, commodity forwards
- `esma_dm/models/spot.py` — `SpotInstrument` (I*): ETCs, spot commodity/FX instruments
- `esma_dm/models/strategy.py` — `StrategyInstrument` (K*): straddles, spreads, multi-leg strategies
- `esma_dm/models/collective.py` — `CollectiveInvestmentInstrument` (C*): ETFs, UCITS, AIFs
- `esma_dm/models/entitlement.py` — `EntitlementInstrument` (R*): subscription rights, mini-futures
- `esma_dm/models/financing.py` — `FinancingInstrument` (L*): repos, securities lending
- `esma_dm/models/referential.py` — `ReferentialInstrument` (T*): currencies, benchmarks used as underlyings
- `esma_dm/models/other.py` — `OtherInstrument` (M*): unclassified and hybrid instruments

Updated `InstrumentMapper.from_row()` to dispatch all 14 CFI category letters to
their dedicated model class (previously F/H/I/J/S all mapped to `DerivativeInstrument`,
C/O/R/K/L/M/T mapped to base `Instrument`).

Updated subtype inheritance in `subtypes.py` to use the new category bases:
- `EquitySwap` now extends `SwapInstrument` (was `DerivativeInstrument`)
- `Swaption` and `EquityOption` now extend `NonStandardDerivativeInstrument`
- `CommodityFuture` now extends `FutureInstrument`
- `FXForward` now extends `ForwardInstrument`
- `MiniFuture` now extends `EntitlementInstrument`

CLI `schema firds --asset` expanded from 8 options to 19, covering all 14 categories:
- `swap`, `future`, `listed-option`, `non-standard`, `forward`, `spot`, `strategy`
- `collective`, `entitlement`, `financing`, `referential`, `other`
- `derivative` and `option` retained for backwards compatibility

`esma_dm/models/__init__.py` updated to export all 12 new model classes.



### Changed - CFI Package Refactoring (ISO 10962)
- Refactored `esma_dm/models/utils/cfi.py` (monolithic, 1219 lines) into a proper Python package at `esma_dm/models/utils/cfi/`.
- One module per ISO 10962 category, each with a focused group enum, attribute value dicts, `decode_attributes()`, and `attribute_labels()` functions:
  - `equity.py` — Category E: EquityGroup (S/P/C/F/D/L/Y/M)
  - `debt.py` — Category D: DebtGroup (B/C/W/T/Y/S/E/G/A/N/D/M)
  - `collective.py` — Category C: CIVGroup (I/H/B/E/S/F/P/M)
  - `entitlements.py` — Category R: EntitlementsGroup (A/S/P/W/F/D/M)
  - `options.py` — Category O: OptionsGroup (C/P)
  - `futures.py` — Category F: FuturesGroup (F/C)
  - `swaps.py` — Category S: SwapsGroup (R/T/E/C/F/M)
  - `non_standard.py` — Category H: NonStandardGroup (R/T/E/C/F/M)
  - `spot.py` — Category I: SpotGroup (F/T)
  - `forwards.py` — Category J: ForwardsGroup (E/F/C/R/T)
  - `strategies.py` — Category K: StrategiesGroup (R/T/E/C/F/Y/M)
  - `financing.py` — Category L: FinancingGroup (L/R/S)
  - `referential.py` — Category T: ReferentialGroup (C/T/R/I/B/D/M)
  - `others.py` — Category M: OthersGroup (C/M)
- `cfi_instrument_manager.py` centralises `CFI` dataclass, `decode_cfi()`, `get_attribute_labels()`, and `group_description()` with dispatch to the correct category module.
- `_shared.py` holds attribute value dicts reused across multiple categories (FORM, DELIVERY variants, EXERCISE_STYLE_EAB, OPTION_STYLE_TYPE, VALUATION_METHOD, debt interest/guarantee/redemption, STANDARDIZATION, PAYOUT_TRIGGER).
- `category.py` holds the `Category` enum (unchanged).
- `__init__.py` re-exports all public symbols; all existing import paths remain backward-compatible.
- Updated `esma_dm/models/utils/__init__.py` to expose all 14 group enums and the new helper functions.
- Old `cfi.py` preserved as `cfi_legacy.py`.

## [0.3.2] - 2026-02-07

### Added - Shared Resources & FITRS File Management
- **Shared File Manager Components** (`esma_dm/file_manager/`):
  - `enums.py`: Unified enums for all ESMA data sources:
    - `FIRDSFileType`, `FITRSFileType` (file type enums)
    - Shared `AssetType`, `InstrumentType` (classification enums)
    - `CommodityBaseProduct`, `OptionType`, `ExerciseStyle`, `DeliveryType`, `BondSeniority`
  - `models.py`: Generic file metadata models:
    - `FileMetadata`: Base class for all file metadata
    - `FIRDSFile`, `FITRSFile`: Specific implementations
  - `downloader.py`: Shared HTTP download operations:
    - Generic file downloading with progress tracking
    - Cache management (list, clear, stats)
    - ZIP file extraction support

- **FITRS File Manager** (`esma_dm/file_manager/fitrs/`):
  - `FITRSFileManager`: Complete FITRS file management:
    - List transparency files with pagination
    - Download latest full files (equity/non-equity)
    - Parse FITRS CSV files
    - Manage local cache with filtering
    - Get statistics by file type and instrument type

- **FITRS CLI Commands** (`esma-dm fitrs`) - **7 Commands** (consistent with FIRDS):
  - `list`: List FITRS files with filtering (by type, instrument, date range)
  - `download`: Download latest FITRS files by instrument type
  - `cache`: List cached FITRS files with size and modification time
  - `fields`: List all field names (columns) in a FITRS CSV file
  - `head`: Display the first N rows of a FITRS CSV file
  - `stats`: Show cache statistics by file type and instrument type
  - `types`: Display all FITRS file types and descriptions

- **FITRS File Type Support**:
  - FULECR (Full Equity Comprehensive Report)
  - DLTECR (Delta Equity Comprehensive Report)
  - FULNCR (Full Non-Equity Comprehensive Report)
  - DLTNCR (Delta Non-Equity Comprehensive Report)
  - FULNCR_NYAR (Non-Equity Subclass Yearly)
  - FULNCR_SISC (Non-Equity Subclass SI)

### Changed
- **CLI Command Restructuring**: Renamed `files` → `firds` for consistency
  - `esma-dm files` → `esma-dm firds` (FIRDS reference data commands)
  - Each data source now has its own specific command group
  - Consistent pattern: `firds`, `fitrs`, `benchmarks` (future)
  - Updated all documentation and examples

- **FIRDS Refactoring** - Updated to use shared resources:
  - `firds/enums.py` → `file_manager/enums.py` (now shared)
  - `firds/models.py` → `file_manager/models.py` (now shared)
  - Updated imports throughout FIRDS module to use shared components
  - Maintained backward compatibility with `FileType` alias

### Technical Improvements
- DRY principle: Eliminated code duplication between data sources
- Extensible architecture: Easy to add new data sources (SSR, Benchmarks)
- **Consistent command structure**: Both FIRDS and FITRS now have identical 7 commands
- Type safety: Full type hints and enum validation throughout
- Unified CSV inspection tools (`fields`, `head`) work across all data sources

## [0.3.1] - 2026-02-07

### Added - Major Restructuring: File Manager Module
- **Comprehensive File Manager Architecture** (`esma_dm/file_manager/`):
  - Replaced `clients/` with unified `file_manager/` module for all file operations
  - Created base `FileManager` class with common operations for all ESMA data sources
  - Implemented complete `FIRDSFileManager` with integrated functionality:
    - File listing with pagination (breaks 1000-record SOLR limit)
    - File downloading with intelligent caching
    - CSV parsing capabilities
    - Metadata extraction from filenames
    - Cache management and statistics
  - Moved all FIRDS components into cohesive structure:
    - `file_manager/firds/manager.py`: Main file manager orchestrator
    - `file_manager/firds/downloader.py`: Download operations
    - `file_manager/firds/parser.py`: CSV parsing logic
    - `file_manager/firds/enums.py`: Type definitions (FileType, AssetType, etc.)
    - `file_manager/firds/models.py`: Data models (FIRDSFile)
    - `file_manager/firds/delta_processor.py`: Delta file processing

- **Enhanced CLI Commands** (`esma-dm files`):
  - `list`: List available files with pagination (fetches all 796+ equity files)
  - `download`: Download latest files by asset type
  - `cache`: List cached files with filtering and statistics
  - `fields`: Display CSV column names
  - `head`: Preview file contents with column selection
  - `types`: **NEW** - List all file types and asset types with descriptions
  - `stats`: **NEW** - Show cache statistics by type and asset

- **File Management Features**:
  - Automatic pagination to fetch unlimited results
  - Date range filtering (date_from, date_to)
  - File type filtering (FULINS/DLTINS/FULCAN)
  - Asset type filtering (C, D, E, F, H, I, J, O, R, S)
  - Metadata extraction (type, asset, date, part numbers)
  - Cache statistics (counts by type/asset, size totals)
  - Intelligent caching with update control

- **Enums and Models Accessible via CLI**:
  - FileType enum (FULINS, DLTINS, FULCAN)
  - AssetType enum (all 10 CFI first characters)
  - CommodityBaseProduct, OptionType, ExerciseStyle, DeliveryType, BondSeniority
  - FIRDSFile model with complete metadata

### Changed
- **Architecture Reorganization**:
  - **`clients/` → `file_manager/`**: All file operations now in dedicated module
  - Separation of concerns clarified:
    - `file_manager/`: File operations (list, download, parse, cache)
    - `clients/`: Data source clients (orchestration + database integration)
    - `storage/`: Database operations (DuckDB, SQL queries)
    - `cli/`: User interface (commands and formatting)
  - Imports updated: `from esma_dm.file_manager import FIRDSFileManager, FileType, AssetType`
  
- **CLI Improvements**:
  - Added `--asset` as required parameter for download command
  - Improved error messages with debug information
  - Better progress indicators for long operations
  - Statistics tables with rich formatting

- **Dependency Updates**:
  - Added `click>=8.0.0` for CLI framework
  - Added `rich>=13.0.0` for terminal formatting

- **Entry Points**:
  - Simplified to single `esma-dm` command
  - Accessible via `python -m esma_dm` or `esma-dm`

### Fixed
- SOLR query pagination to fetch more than 1000 results
- File metadata extraction from FIRDS filenames
- Date filtering properly applied to SOLR queries
- Path validation for both relative and absolute paths
- Rich table compatibility issues

### Performance
- Can fetch all 796+ equity files instead of 100-record limit
- Automatic pagination handles large result sets efficiently  
- Cache statistics computed on-demand

### Developer Experience
- Clear module boundaries and responsibilities
- Easy to extend with new data sources (FITRS, SSR, Benchmarks)
- All file operations accessible from CLI for testing
- Type definitions and models exported from file_manager

## [0.3.0] - 2026-01-25

### Added
- **Centralized Configuration Management**:
  - Created specialized configuration classes (`FIRDSConfig`, `FITRSConfig`, `DatabaseConfig`)
  - Added mode-specific configuration factory functions (`get_firds_config()`, etc.)
  - Implemented smart defaults with validation (e.g., `validate_limit()`, `get_date_range()`)
  - Mode-aware behavior: current mode optimized for performance, history mode for accuracy
- **Component-Based Architecture**:
  - Transformed FIRDSClient from "God Object" pattern to clean component composition
  - Added component access properties: `firds.download`, `firds.parse`, `firds.store`, `firds.delta`
  - Reduced API surface from 20+ methods to 6 high-level orchestration methods
  - Self-documenting API through logical component grouping
  - Clear separation of concerns: download vs parse vs storage vs delta processing
- **High-Level Orchestration Methods**:
  - `initialize_database()`: Database schema initialization
  - `build_reference_database()`: Complete download + index workflow
  - `get_reference_data()`: Quick ISIN lookup
  - `query_database()`: Custom SQL execution
  - `get_database_stats()`: Comprehensive statistics
  - `process_deltas()`: History mode incremental updates
- **Short name field support** for all instrument types:
  - Added `short_name` column to instruments database schema
  - Enhanced field extraction to include `RefData_FinInstrmGnlAttrbts_ShrtNm` (100% populated)
  - Updated bulk inserters to include short_name in database operations
  - Extended reference API to return short_name in instrument lookups
  - Provides concise, readable instrument identifiers (e.g., "NA/Swap OIS EUR 20290806")

### Changed
- **API Design Philosophy**:
  - **Before**: Many delegated methods on main client class (firds.get_latest_full_files())
  - **After**: Component-specific operations (firds.download.get_latest_full_files())
  - Maintains backward compatibility for critical methods like `index_cached_files()`
  - Component boundaries align with actual responsibilities and use cases
- **Project Structure Reorganization**:
  - Removed unused directories (`esma_dm/data/`, `esma_dm/downloads/`)
  - Consolidated to single data location (`downloads/data/`) with organized subdirectories
  - Added cache directory for temporary operations (`downloads/data/cache/`)
  - Database location remains in `storage/duckdb/database/` as designed
- **Import Path Simplification**:
  - Eliminated complex relative imports (`from ...utils.validators import`)
  - Standardized to clean absolute imports (`from esma_dm.utils.validators import`)
  - Removed all fragile three-dot import patterns across codebase
  - Enhanced IDE navigation and import resolution
- **Configuration System Overhaul**:
  - Moved `config.py` to `config/` module with base configuration and specialized registries
  - Eliminated hardcoded defaults scattered throughout codebase
  - Replaced `date_from='2024-01-01'` and `limit=1000` with centralized configuration
  - Updated all clients to use configuration classes for consistent behavior

### Fixed
- **FIRDS data model alignment** with actual ESMA structure:
  - Corrected equity, debt, and swap instrument models to use real FIRDS fields
  - Fixed database schema column mappings (`asset_type` → `instrument_type`)
  - Updated bulk inserters to handle processed DataFrame fields correctly
  - Eliminated fictional/assumed fields in favor of actual ESMA data structure
  - Resolved foreign key constraint violations through proper insertion order
- **Database operation improvements**:
  - Fixed column ordering issues in bulk INSERT operations
  - Corrected field mapping between CSV extraction and database schema
  - Enhanced error handling for data type conversions
  - Improved debug logging for troubleshooting data loading issues
- **Import and dependency issues**:
  - Resolved circular import problems in module initialization
  - Fixed configuration access patterns across all components
  - Eliminated import errors from complex relative path chains

### Improved
- **Code Maintainability**:
  - 0 complex relative imports across all analyzed modules
  - Consistent configuration patterns throughout codebase
  - Self-documenting configuration with clear mode-specific behavior
  - Better separation of concerns between components
- **Developer Experience**:
  - Cleaner project structure with logical organization
  - Simplified imports that IDEs can properly resolve
  - Centralized configuration eliminates hunting for hardcoded values
  - Mode-specific optimizations for different use cases

## [Unreleased]

### Added

#### Package Infrastructure (2026-01-11)
- **Enhanced setup.py** with proper dependencies and metadata:
  - Added DuckDB and NumPy as required dependencies
  - Updated version to 0.2.0 reflecting architectural improvements
  - Added Python 3.13 support and enhanced dev dependencies
  - Enhanced keywords and project classifiers
- **Virtual environment testing**:
  - Created `scripts/test_virtual_env.py` for clean environment validation
  - Comprehensive integration testing ensuring all components work together
  - Backwards compatibility verification for existing user code
- **Documentation updates**:
  - Updated README.md with new modular architecture overview
  - Enhanced Copilot instructions with utility module patterns
  - Comprehensive CHANGELOG documentation of all changes

### Changed

#### Architectural Refactoring (2026-01-11)
- **Created centralized utility modules** for code reusability:
  - `esma_dm/utils/validators.py`: ISO standard validators (ISIN/ISO 6166, LEI/ISO 17442, CFI/ISO 10962, MIC/ISO 10383)
  - `esma_dm/utils/constants.py`: ESMA URL constants, file patterns, default settings
  - `esma_dm/utils/query_builder.py`: Reusable SQL query patterns for database operations
  - `esma_dm/utils/shared_utils.py`: Common utilities for file operations and XML parsing
  - `esma_dm/utils/__init__.py`: Unified export interface for all utilities
- **Enhanced configuration system**:
  - Added URL constants to Config class (FIRDS_BASE_URL, FITRS_BASE_URL, SSR_BASE_URL, BENCHMARKS_BASE_URL)
  - Added mode validation in Config.__post_init__()
  - Added get_database_path() helper method for consistent database path resolution
  - Added mode parameter with validation against DATABASE_MODES constant
- **Modularized DuckDB storage backend** (1293→9 lines + 5 modules):
  - `esma_dm/storage/duckdb/connection.py`: Database connection and initialization (140 lines)
  - `esma_dm/storage/duckdb/operations.py`: Bulk insert and update operations (285 lines)
  - `esma_dm/storage/duckdb/queries.py`: Instrument retrieval and search queries (350 lines) 
  - `esma_dm/storage/duckdb/versioning.py`: Delta processing and version management (250 lines)
  - `esma_dm/storage/duckdb/__init__.py`: Unified DuckDBStorage interface (130 lines)
  - `esma_dm/storage/duckdb_store.py`: Compatibility import layer (9 lines)
  - **Benefits**: Better separation of concerns, easier testing, reduced file complexity
  - **Backward compatibility**: All existing imports continue to work unchanged
- **Modularized FIRDS client** (1116→6 focused modules):
  - `esma_dm/clients/firds/enums.py`: File types, asset types, record types (45 lines)
  - `esma_dm/clients/firds/models.py`: Data models and validation (120 lines)
  - `esma_dm/clients/firds/downloader.py`: File download and caching logic (280 lines)
  - `esma_dm/clients/firds/parser.py`: CSV parsing and instrument mapping (200 lines)
  - `esma_dm/clients/firds/delta_processor.py`: Delta file processing (150 lines)
  - `esma_dm/clients/firds/client.py`: Main client orchestrator (265 lines)
  - **Benefits**: Independent testing, better maintainability, clear separation of concerns
- **Organized storage directory structure**:
  - `esma_dm/storage/schema/`: Table definitions and database schemas
  - `esma_dm/storage/bulk/`: Bulk insert operations and vectorization
  - `esma_dm/storage/fitrs/`: FITRS-specific storage components
  - **Benefits**: Logical grouping, easier navigation, cleaner imports
- **Eliminated code duplication**:
  - Created QueryBuilder utility to extract recurring SQL patterns from storage classes
  - Removed duplicate validator methods from clients/firds.py and firds.py (now delegate to utils.validators)
  - Removed hardcoded URLs from all clients (FIRDS, FITRS, SSR) - now use constants module
  - Removed hardcoded SQL queries - now use QueryBuilder for consistency
  - Centralized CFI asset type mappings in QueryBuilder.ASSET_TYPE_TABLES
- **Updated imports across codebase**:
  - All clients now import from esma_dm.utils.constants for URLs
  - All validators now import from esma_dm.utils.validators
  - Project config.py now imports from package constants with fallback
- **Backward compatibility preserved**:
  - FIRDSClient.validate_isin/lei/cfi() kept as deprecated wrappers (delegate to utils)
  - All existing public APIs unchanged

### Deprecated

#### Legacy FIRDS Module (2026-01-11)
- **Deprecated esma_dm/firds.py module** in favor of esma_dm/clients/firds.py:
  - Added deprecation warnings when importing or instantiating legacy FIRDSClient
  - Updated docstrings with migration guidance and feature comparison
  - Legacy module will be removed in next major version (v1.0.0)
- **Migration path**:
  - OLD: `from esma_dm.firds import FIRDSClient`
  - NEW: `from esma_dm.clients.firds import FIRDSClient`
  - RECOMMENDED: `from esma_dm import FIRDSClient` (imports from clients automatically)
- **Benefits of new client**:
  - Mode-based operation (current vs history)
  - Enhanced caching and performance
  - Better error handling and validation
  - Delta file processing capabilities (DLTINS support)
  - ESMA Section 8.2 compliance for historical tracking

### Added

#### Mode-Based Operation (2026-01-11)
- Added mode parameter to FIRDSClient and DuckDBStorage:
  - `mode='current'` (default): Latest FULINS snapshots, optimized for current data queries
  - `mode='history'`: Full version tracking with DLTINS delta processing
- Separate databases prevent interference:
  - Current mode: firds_current.duckdb (9 core columns, no historical tracking)
  - History mode: firds_history.duckdb (17 columns with version management)
- Mode-specific features:
  - Current mode: Simple snapshot workflow with minimal overhead
  - History mode: ESMA Section 8.2 compliance with version_number, valid_from/to dates, latest_record_flag
  - process_delta_files() restricted to history mode only
- Default caching enabled for development:
  - All download methods default to update=False (use cached files)
  - Significantly faster iteration during development
  - Set update=True explicitly when fresh data needed

#### FIRDS Historical Database Support (2026-01-11)
- Complete implementation of ESMA65-8-5014 Section 8 historical tracking requirements:
  - Added temporal fields to instruments table: valid_from_date, valid_to_date, latest_record_flag, record_type, version_number
  - Added source tracking: source_file_type, last_update_timestamp, inconsistency_indicator
  - Created cancellations table for FULCAN file support (8 fields + 2 indexes)
  - Created instrument_history table for full version tracking (12 fields + 4 indexes with UNIQUE(isin, version_number))
  - Added version_number field to all 9 asset-specific tables (equity, debt, futures, options, swaps, forwards, rights, civ, spot)
  - Added 6 temporal indexes: idx_instruments_latest (filtered), idx_instruments_valid_from, idx_instruments_valid_to, idx_instruments_record_type, idx_cancellations_isin, idx_cancellations_date
- Temporal query methods per ESMA Section 9:
  - get_latest_instruments(): Query current versions with latest_record_flag=TRUE
  - get_instruments_active_on_date(): Query instruments trading on specific date
  - get_instrument_state_on_date(): Retrieve historical instrument state on any date
  - get_instrument_version_history(): Full version history for an ISIN
  - get_modified_instruments_since(): Track changes from specific date
  - get_cancelled_instruments(): Query FULCAN cancellations
- Delta file processing (Phase 2):
  - XML parsing extracts record type wrappers: <NewRcrd>, <ModfdRcrd>, <TermntdRcrd>, <CancRcrd>
  - process_delta_record() implements ESMA Section 8.2 version management:
    * NEW: Insert new version (handle late records)
    * MODIFIED: Close previous version, insert new (valid_to_date = new valid_from - 1 day)
    * TERMINATED: Archive and mark as terminated
    * CANCELLED: Move to cancellations table
  - Automatic version numbering and valid_from_date/valid_to_date management
  - History archival before updates
- Schema supports delta record types: NEW, MODIFIED, TERMINATED, CANCELLED
- Database now maintains full audit trail per ESMA regulatory guidance

#### Full MiFIR Transparency Support (2026-01-11)
- Complete ESMA65-8-5240 transparency requirements implementation
- Extended transparency table schema with 15+ MiFIR-compliant fields:
  - Most relevant market: most_relevant_market_id, most_relevant_market_avg_daily_trades
  - Application periods: application_period_from, application_period_to (from April 2025 files)
  - Non-equity thresholds: pre_trade_lis_threshold, post_trade_lis_threshold, pre_trade_ssti_threshold, post_trade_ssti_threshold
  - Metadata: instrument_type, file_type tracking
- New subclass_transparency table for sub-class level results:
  - Support for FULNCR_NYAR (yearly non-equity sub-class results)
  - Support for FULNCR_SISC (SI non-equity sub-class results)
  - Segmentation criteria stored as JSON (30+ criteria types: BSPD, SBPD, FSPD, TTMB, etc.)
  - Fields: asset_class, sub_asset_class_code, sub_asset_class_description, calculation_type, methodology
- Delta file support:
  - DLTECR (equity delta): Incremental ISIN-level updates
  - DLTNCR (non-equity delta): Incremental ISIN-level updates
  - INSERT OR REPLACE logic for proper incremental updates
- Updated column mapping: FrDt/ToDt → reporting_period_from/to, Id_2 → most_relevant_market_id
- Enhanced query methods with new filters: instrument_type, most_relevant_market, methodology
- New query_subclass_transparency() method for sub-class level queries
- insert_subclass_transparency_data() method with JSON segmentation criteria handling
- Full file type support: FULECR, FULNCR, DLTECR, DLTNCR, FULNCR_NYAR, FULNCR_SISC
- Extended index() method to handle all file types and sub-class data
- TransparencyAPI.query_subclass() for user-facing sub-class queries
- Transparency utility enums (transparency_enums.py):
  - Methodology enum: SINT, YEAR, ESTM, FFWK with descriptions
  - InstrumentClassification enum: SHRS, DPRS, ETFS, OTHR with descriptions
  - FileType enum: All 6 FITRS file types with descriptions and type checks
  - SegmentationCriteria enum: 40+ segmentation criteria codes with descriptions and categories
  - Helper functions: format_methodology_info(), format_classification_info(), format_segmentation_info()
  - Client utility methods: get_methodology_info(), get_classification_info(), list_methodologies(), list_classifications(), list_file_types()

#### FITRS Transparency Data Support (2026-01-11)
- Separate FITRS database (fitrs.db) for transparency data storage
- Support for FULECR (equity) and FULNCR (non-equity) transparency files
- Cross-database query support via DuckDB ATTACH feature
- New transparency API: edm.transparency(isin) parallel to edm.reference(isin)
- TransparencyAPI class with index(), query(), and attach_firds() methods
- FITRSStorage backend with dedicated schema for transparency metrics
- Database schema includes: transparency, equity_transparency, non_equity_transparency, subclass_transparency, transparency_metadata tables
- index_transparency_data() method to download and process FITRS files
- Query methods with filters: liquid_only, instrument_type, min_turnover
- Cross-database SQL queries joining FIRDS reference data with transparency metrics
- Example script: examples/06_transparency_data.py demonstrating all features

#### Subtype Output Models (2026-01-10)
- Created 8 specialized output models for major instrument subtypes
- Models based on actual FIRDS fields verified from CSV data
- EquitySwap (SE*): 28 fields including underlying_isin, interest_rate_reference_name
- Swaption (HR*): 38 fields including option_type, strike_price, first_leg_interest_rate
- EquityOption (HE*): 33 fields including option_type, strike_price, underlying_isin
- MiniFuture (RF*): 50 fields including 19 commodity fields and 2 FX fields
- StructuredEquity (EY*): 28 fields with underlying references and commodity attributes
- StructuredDebt (DE*): 29 fields with interest rates and debt seniority
- CommodityFuture (FC*): 56 fields with extensive commodity taxonomy (33 fields)
- FXForward (JF*): 22 fields including fx_type and notional currencies
- All models include from_dict() classmethod to parse attributes JSON
- Added subtypes() discovery method to ReferenceAPI
- Verification tools: verify_subtype_fields.py, map_firds_actual_fields.py
- 8 models cover 12.4M instruments (73% of 16.9M total FIRDS instruments)

#### Database Initialization and Verification (2026-01-10)
- Enhanced initialize() method with automatic schema verification
- Verifies database structure matches data models on every initialization
- Non-destructive: if database exists, verifies schema without reinitializing
- verify_only parameter for explicit schema verification without modifications
- Returns detailed status including tables created/verified and any errors
- Checks for missing tables and columns against expected schema
- Validates all 12 tables: instruments, listings, 9 asset-specific tables, metadata
- Enhanced drop() method with safety confirmation requirement
- Drop method now requires confirm=True parameter to prevent accidental data loss
- Returns detailed status including file size and deletion confirmation
- Properly closes database connections before deletion
- Enhanced index_cached_files() method with filtering and selection options
- New asset_type parameter to filter by specific asset type (C, D, E, F, H, I, J, O, R, S)
- New latest_only parameter (default: True) to automatically select most recent files
- New file_type parameter to choose FULINS (snapshots) or DLTINS (deltas)
- Automatically detects and skips older file versions when latest_only=True
- Returns detailed statistics: instruments, listings, files processed/skipped, asset types
- Tools folder cleanup: Removed 9 temporary test/debug scripts
- Kept 3 useful tools: analyze_field_coverage.py, display_database_schema.py, display_schemas.py
- Added tools/README.md documenting purpose and usage of each tool
- Updated documentation with clear workflow: Install → Initialize → Download → Load → Query
- Database management section with verification, drop/rebuild, and update examples
- New example scripts: 00_initialize_database.py, 01_drop_database.py, 02_index_with_filters.py

#### ISO 10962 CFI Standard Compliance (2026-01-10)
- Fixed config.py asset type descriptions to match ISO 10962 CFI categories
- Corrected asset type names: H (Non-Standardized Derivatives), I (Spot), J (Forwards), O (Options), R (Entitlements)
- Updated table name mappings to singular form following CFI standard
- Ensures consistency with CFI classification implementation in models/utils/

#### Listings Table Refactoring (2026-01-06)
- **Breaking Change**: Separated trading venue listings into dedicated `listings` table
- Normalized one-to-many relationship: one instrument can have multiple venue listings
- Removed listing fields from all asset-specific tables: trading_venue_id, first_trade_date, termination_date, issuer_request
- Asset tables now contain only asset-specific attributes
- Auto-incrementing ID sequence for listings table
- Listings table includes: admission_approval_date, request_for_admission_date
- Database now has 12 tables: instruments, listings, 9 asset-specific tables, metadata
- Properly handles ISINs with multiple venue listings (3.29M listings for 2.37M instruments)
- Technical metadata fields (competent_authority, publication_date) remain in asset tables
- Updated all 10 bulk inserters to remove listing field extraction/insertion
- Updated all 10 schema table definitions to remove listing columns
- Changed delete_csv default to False to support field coverage analysis
- Tool cleanup: Removed outdated analyze_schema_coverage.py, test_firds_enhancements.py, inspect_firds_files.py
- New tool: display_database_schema.py for inspecting actual DuckDB table structure
- Field coverage analysis now shows 72.7% average coverage across all asset types

#### Comprehensive Field Coverage Improvements (2026-01-05)
- Added trading_venue_id extraction to all asset types (was only in swaps)
- Added technical metadata fields to all asset types: issuer_request, competent_authority, publication_date
- Enhanced debt instrument fields: maturity_date, total_issued_nominal_amount, nominal_value_per_unit, debt_seniority
- Enhanced option instrument fields: option_type (CALL/PUT), option_exercise_style (AMER/EURO), strike_price
- Updated all schema tables with new columns
- Updated all bulk inserters with proper column pattern matching for RefData_ prefixed fields
- Field coverage tool for analyzing data extraction quality

#### Database Management Methods (2026-01-05)
- Added db.initialize(mode) method: Initialize database with 'current' (FULINS) or 'delta' (DLTINS) mode
- Added db.drop() method: Remove database and close connections
- Added db.update(mode, from_date, to_date) method: Update database with new data (planned)
- Separate listings table for trading venue data (one-to-many relationship with instruments)
- Listings table removed from asset-specific schemas
- Automatic DLTINS file exclusion during initial data load

#### Reference API (2026-01-04)
- Hierarchical reference API for convenient instrument queries
- Callable interface: `edm.reference('ISIN')` for direct lookups
- Asset type queries: `edm.reference.swap.types()` for CFI code discovery
- Statistics methods: `.count()`, `.sample()` per asset type
- Global methods: `.summary()` and `.types()` across all asset types
- Integrated CFI descriptions in type queries
- Support for all 10 asset types: equity, debt, civ, futures, options, swap, referential, rights, spot, forward

#### CFI Classification System (2026-01-03)
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

#### Column Mapping Improvements (2026-01-04)
- Enhanced swap instruments column mappings for better data extraction
- Added support for `AsstClssSpcfcAttrbts_Intrst_IntrstRate_*` fields
- Improved trading venue, first trade date, and termination date field detection
- Added `TradgVnRltdAttrbts_*` column pattern matching
- Interest rate and underlying index fields now correctly extracted

#### ISO 10962 Compliance - Asset Type Mapping (2026-01-03)
- Corrected FIRDS file type mappings to match ISO 10962 CFI standard
- I type: Renamed from "Indices" to "Spot" (spot contracts and indices)
- J type: Renamed from "Listed Options" to "Forwards" (forward contracts and warrants)
- Table names: index_instruments → spot_instruments, listed_option_instruments → forward_instruments
- Schema functions: create_index_table → create_spot_table, create_listed_option_table → create_forward_table
- Bulk insert methods: insert_indices → insert_spots, insert_listed_options → insert_forwards
- Updated documentation and routing logic to align with CFI Category enum

#### Complete Asset Type Support (2026-01-03)
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

#### Storage Architecture (2026-01-06)
- Expanded from 5 tables to 12 tables for complete asset type coverage
- Separate listings table for normalized one-to-many venue relationships
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
