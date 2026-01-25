# Data Directory

This directory contains the ESMA data databases and cached files.

## Structure

- `firds/` - FIRDS (Financial Instrument Reference Data) files and databases
- `fitrs/` - FITRS (Financial Instrument Transparency System) files and databases

## Database Files

- `firds_current.duckdb` - Current mode database (latest snapshots only)
- `firds_history.duckdb` - History mode database (full version tracking)

## Generated Files

This directory is automatically created and managed by the ESMA Data Manager.
Do not manually edit the database files.