# Schema Introspection Summary

## Overview

The esma-dm data models now include comprehensive schema introspection capabilities. Every model class has a `get_schema()` class method that returns detailed field information including types, descriptions, and required status.

## Available Methods

### get_schema()

All model classes support the `get_schema()` class method:

```python
from esma_dm.models import DebtInstrument

schema = DebtInstrument.get_schema()

# Returns a dictionary:
# {
#     'field_name': {
#         'type': 'str|float|int|date|bool|ClassName',
#         'description': 'Field description',
#         'required': True|False  # Optional, defaults to False
#     },
#     ...
# }
```

### Supported Classes

- `Instrument` (base class)
- `TradingVenueAttributes`
- `TechnicalAttributes`
- `DebtInstrument`
- `EquityInstrument`
- `DerivativeInstrument`
- `OptionAttributes`
- `FutureAttributes`

## Usage Examples

### Basic Schema Inspection

```python
from esma_dm.models import EquityInstrument

schema = EquityInstrument.get_schema()

# Iterate over fields
for field_name, field_info in schema.items():
    print(f"{field_name}:")
    print(f"  Type: {field_info['type']}")
    print(f"  Description: {field_info['description']}")
    if field_info.get('required'):
        print(f"  Required: Yes")
```

### Check Required Fields

```python
required_fields = [
    name for name, info in schema.items()
    if info.get('required', False)
]
print(f"Required fields: {', '.join(required_fields)}")
# Output: Required fields: isin, full_name
```

### Field Type Checking

```python
# Get fields of specific type
date_fields = [
    name for name, info in schema.items()
    if info['type'] == 'date'
]
print(f"Date fields: {', '.join(date_fields)}")
```

### Compare Models

```python
from esma_dm.models import Instrument, DebtInstrument

base_schema = Instrument.get_schema()
debt_schema = DebtInstrument.get_schema()

# Find debt-specific fields
debt_specific = set(debt_schema.keys()) - set(base_schema.keys())
print(f"Debt-specific fields: {', '.join(debt_specific)}")
```

## Tools

Two command-line tools are provided for schema inspection:

### 1. display_schemas.py

Displays complete schema information for all models:

```bash
python tools/display_schemas.py
```

Output includes:
- All fields with types and descriptions
- Required field markers
- Comparison between base and derived models
- Asset type to model mapping
- Summary statistics

Output saved to: `docs/SCHEMA_REFERENCE.txt`

### 2. analyze_schema_coverage.py

Analyzes actual FIRDS data files and shows schema coverage:

```bash
python tools/analyze_schema_coverage.py
```

Provides:
- Analysis of each data file (columns, records, model class)
- Top populated columns per file
- Summary by asset type
- Overall statistics
- Model coverage mapping

## Schema Statistics

### Base Instrument
- **Fields**: 12
- **Required**: isin, full_name
- **Used for**: C (Collective Investment), O (Others), R (Referential)

### DebtInstrument
- **Total Fields**: 24 (12 base + 12 specific)
- **Debt-specific**: 12 fields
- **Used for**: D (Debt instruments)

### EquityInstrument
- **Total Fields**: 17 (12 base + 5 specific)
- **Equity-specific**: 5 fields
- **Used for**: E (Equities)

### DerivativeInstrument
- **Total Fields**: 27 (12 base + 15 specific)
- **Derivative-specific**: 15 fields
- **Used for**: F (Futures), I (Options), J (Forwards), S (Swaps), H (Others)

### Nested Attributes
- **TradingVenueAttributes**: 6 fields
- **TechnicalAttributes**: 4 fields
- **OptionAttributes**: 5 fields
- **FutureAttributes**: 3 fields

## Integration with InstrumentMapper

The schema information is consistent with the `InstrumentMapper` field mapping:

```python
from esma_dm.models import InstrumentMapper, DebtInstrument
import pandas as pd

# Load data
df = pd.read_csv('FULINS_D_data.csv')

# Convert to models
instruments = InstrumentMapper.from_dataframe(df)

# All fields in schema are accessible on the model
debt_schema = DebtInstrument.get_schema()
first_bond = instruments[0]

for field_name in debt_schema.keys():
    if hasattr(first_bond, field_name):
        value = getattr(first_bond, field_name)
        print(f"{field_name}: {value}")
```

## Benefits

1. **Documentation**: Self-documenting models with embedded field descriptions
2. **Validation**: Can build validators based on schema (required fields, types)
3. **UI Generation**: Generate forms or displays dynamically from schema
4. **API Documentation**: Generate API docs automatically
5. **Data Quality**: Compare raw data against expected schema
6. **Testing**: Generate test fixtures based on schema requirements

## Future Enhancements

Possible extensions to the schema system:

- Add validation rules (min/max, regex patterns, enum values)
- Add field constraints (foreign keys, uniqueness)
- Add deprecation warnings for fields
- Add version information per field
- Generate JSON Schema for API documentation
- Generate Pydantic models from schema
