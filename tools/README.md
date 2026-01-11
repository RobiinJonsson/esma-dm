# Tools

Utility scripts for analyzing and inspecting the ESMA Data Manager package.

## Available Tools

### `analyze_field_coverage.py`

Analyzes field coverage and data quality by comparing database content with CSV source files.

**Purpose:**
- Measures how many fields are successfully extracted from FIRDS files
- Compares database columns vs CSV columns for each asset type
- Helps identify missing or unused fields

**Usage:**
```bash
python tools/analyze_field_coverage.py
```

**Output:**
- Coverage percentage per asset type
- Lists of missing fields
- Overall data extraction quality metrics

---

### `display_database_schema.py`

Shows the actual DuckDB database schema with all tables and columns.

**Purpose:**
- Inspect current database structure
- Verify table definitions
- Check column names and types
- Useful for debugging schema issues

**Usage:**
```bash
python tools/display_database_schema.py
```

**Output:**
- List of all tables in database
- Column definitions for each table
- Data types and constraints

---

### `display_schemas.py`

Shows Python model schemas and data structures from the codebase.

**Purpose:**
- Display expected schema from Python models
- Compare model definitions with actual database
- Understand data model structure

**Usage:**
```bash
python tools/display_schemas.py
```

**Output:**
- Python dataclass schemas
- Model field definitions
- Expected structure vs actual structure

---

## When to Use These Tools

- **After schema changes**: Verify database structure matches models
- **Data quality issues**: Check field coverage to identify extraction problems
- **Schema debugging**: Compare actual vs expected table structures
- **Documentation**: Generate schema documentation for reference
