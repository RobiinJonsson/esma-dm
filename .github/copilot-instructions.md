# GitHub Copilot Instructions for esma-dm

## Project Overview
Python package for accessing ESMA (European Securities and Markets Authority) published data including FIRDS, FITRS, SSR, and Benchmarks.

## Code Style Rules

### Documentation
- No emojis in any code, comments, or documentation
- Professional and concise language only
- Use clear, technical descriptions
- Follow Google-style docstrings for Python

### Markdown Files
- Only two .md files allowed at project root: README.md and CHANGELOG.md
- Folders may have their own README.md only if necessary
- CHANGELOG.md must have timestamped entries (YYYY-MM-DD format)
- README.md describes package usage and API

### Python Code
- Follow PEP 8 style guidelines
- Use type hints for all function parameters and return values
- Use dataclasses for structured data
- Use Enums for fixed sets of values
- Validate inputs using proper validation methods
- Keep methods focused and single-purpose

### Architecture
- Modular design with separate clients for each ESMA dataset
- Shared utilities in utils.py
- Configuration management via Config class
- Caching enabled by default for performance
- Error handling with informative messages

### Data Standards
- Follow ISO standards: ISO 6166 (ISIN), ISO 17442 (LEI), ISO 10962 (CFI)
- Follow RTS 23 specifications for FIRDS data
- Use pandas DataFrames for tabular data
- Parse dates to datetime objects
- Normalize data to typed models where appropriate

### Testing
- Write unit tests for all new functionality
- Test validation methods thoroughly
- Include example usage in tests
- Ensure backward compatibility

### Naming Conventions
- Classes: PascalCase (e.g., FIRDSClient)
- Functions/methods: snake_case (e.g., get_file_list)
- Constants: UPPER_SNAKE_CASE (e.g., BASE_URL)
- Private methods: prefix with underscore (e.g., _parse_xml)

### Import Organization
1. Standard library imports
2. Third-party imports (pandas, requests, etc.)
3. Local imports
4. Use absolute imports, not relative

### Error Handling
- Raise exceptions for invalid inputs
- Log errors with appropriate level
- Provide helpful error messages
- Clean up resources in finally blocks

## Project-Specific Rules

### FIRDS Client
- Support both FULINS (full) and DLTINS (delta) files
- Validate ISIN, LEI, CFI codes before processing
- Use AssetType enum for type safety
- Filter files by asset_type and file_type parameters

### FITRS Client
- Support multiple instrument types
- Handle DVCAP data separately
- Provide transparency metrics

### Data Models
- Use dataclasses with type hints
- Include get_schema() methods for introspection
- Map CFI codes to appropriate model classes
- Handle optional fields with Optional[T]

### Validation
- Implement static validation methods on client classes
- Validate format and structure, not business logic
- Return bool for validation methods
- Document expected formats in docstrings

### Performance
- Cache downloaded files in project downloads folder
- Use date-based filtering to reduce API calls
- Parse XML efficiently with lxml
- Batch operations where possible

## Examples to Follow

### Good Method Signature
```python
def get_file_list(
    self,
    file_type: Optional[str] = None,
    asset_type: Optional[str] = None
) -> pd.DataFrame:
    \"\"\"
    Retrieve list of available FIRDS files.
    
    Args:
        file_type: Filter by file type (FULINS or DLTINS)
        asset_type: Filter by asset type (C, D, E, F, H, I, J, O, R, S)
    
    Returns:
        DataFrame containing file metadata
    
    Example:
        >>> firds = FIRDSClient()
        >>> files = firds.get_file_list(file_type='FULINS', asset_type='E')
    \"\"\"
```

### Good Enum Definition
```python
class AssetType(Enum):
    \"\"\"CFI first character representing asset types (ISO 10962).\"\"\"
    EQUITY = "E"  # Equities (shares, units)
    DEBT = "D"    # Debt instruments (bonds, notes)
```

### Good Dataclass
```python
@dataclass
class FIRDSFile:
    \"\"\"Metadata for a FIRDS file.\"\"\"
    file_name: str
    file_type: str
    publication_date: str
    download_link: str
    asset_type: Optional[str] = None
```

## What to Avoid
- Emojis anywhere in the project
- Verbose or chatty language
- Multiple markdown documentation files
- Hardcoded paths or credentials
- Catching exceptions without logging
- Methods that do too many things
- Magic numbers without constants
- Unclear variable names
