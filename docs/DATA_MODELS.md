# ESMA Data Models

Normalized Python data models for ESMA FIRDS reference data.

## Overview

The models package provides a clean, Pythonic interface to ESMA's Financial Instruments Reference Data System (FIRDS). Instead of working with raw XML column names like `RefData_FinInstrmGnlAttrbts_FullNm`, you can access data through clean properties like `instrument.full_name`.

## Features

- **Type-safe models**: Dataclasses with proper type hints
- **Asset-specific schemas**: Different models for debt, equity, and derivatives
- **Automatic normalization**: Convert raw ESMA data to clean Python objects
- **Computed properties**: Convenient methods like `is_fixed_rate`, `has_voting_rights`
- **Nested attributes**: Organized grouping of related fields
- **Format agnostic**: Works with both FULINS and DLTINS files

## Model Hierarchy

```
Instrument (base class)
├── DebtInstrument (bonds, notes, etc.)
├── EquityInstrument (shares, stocks, etc.)
└── DerivativeInstrument (futures, options, swaps, etc.)
    ├── OptionAttributes
    └── FutureAttributes
```

## Usage

### Basic Example

```python
from esma_dm.models import InstrumentMapper
import pandas as pd

# Load raw FIRDS data
df = pd.read_csv('FULINS_F_20240601_01of01_data.csv')

# Convert to normalized models
instruments = InstrumentMapper.from_dataframe(df)

# Access data through clean properties
for inst in instruments[:5]:
    print(f"ISIN: {inst.isin}")
    print(f"Name: {inst.full_name}")
    print(f"Type: {inst.asset_type}")
    print(f"Currency: {inst.notional_currency}")
    print()
```

### Working with Derivatives

```python
from esma_dm.models import DerivativeInstrument

# Filter derivative instruments
derivatives = [i for i in instruments if isinstance(i, DerivativeInstrument)]

for deriv in derivatives:
    print(f"{deriv.full_name}")
    
    # Check derivative type
    if deriv.is_future:
        print(f"  Future expiring: {deriv.expiry_date}")
        if deriv.future_attrs:
            print(f"  Delivery: {deriv.future_attrs.delivery_type}")
    
    elif deriv.is_option:
        print(f"  Option expiring: {deriv.expiry_date}")
        if deriv.option_attrs:
            print(f"  Type: {deriv.option_attrs.option_type}")
            print(f"  Strike: {deriv.option_attrs.strike_price}")
    
    # Check if commodity derivative
    if deriv.is_commodity_derivative:
        print(f"  Commodity: {deriv.base_product} / {deriv.sub_product}")
```

### Working with Debt Instruments

```python
from esma_dm.models import DebtInstrument

# Filter debt instruments
bonds = [i for i in instruments if isinstance(i, DebtInstrument)]

for bond in bonds:
    print(f"{bond.full_name}")
    print(f"  Maturity: {bond.maturity_date}")
    print(f"  Total Issued: {bond.total_issued_nominal_amount:,.2f}")
    
    # Check interest rate type
    if bond.is_fixed_rate:
        print(f"  Fixed Rate: {bond.fixed_rate}%")
    elif bond.is_floating_rate:
        print(f"  Floating Rate: {bond.floating_rate_reference_index}")
        print(f"  Spread: {bond.floating_rate_basis_points} bps")
```

### Working with Equity Instruments

```python
from esma_dm.models import EquityInstrument

# Filter equity instruments
equities = [i for i in instruments if isinstance(i, EquityInstrument)]

for equity in equities:
    print(f"{equity.full_name}")
    print(f"  Voting Rights: {'Yes' if equity.has_voting_rights else 'No'}")
    print(f"  Redeemable: {'Yes' if equity.is_redeemable else 'No'}")
    print(f"  Dividend Frequency: {equity.dividend_payment_frequency}")
```

## Model Reference

### Base Instrument

All instruments inherit from the `Instrument` base class:

**Core Identifiers:**
- `isin` - International Securities Identification Number
- `full_name` - Full instrument name
- `short_name` - Short instrument name
- `classification_type` - CFI code (ISO 10962)

**Properties:**
- `asset_type` - First character of CFI code (D=Debt, E=Equity, F=Futures, etc.)
- `instrument_category` - Second character of CFI code

**Issuer & Currency:**
- `issuer` - LEI code of the issuer
- `notional_currency` - Currency code (ISO 4217)
- `commodity_derivative_indicator` - Boolean string for commodity derivatives

**Nested Attributes:**
- `trading_venue` - TradingVenueAttributes dataclass
- `technical` - TechnicalAttributes dataclass

**DLTINS Fields (delta files only):**
- `record_type` - RecordType enum (MODIFIED, NEW, TERMINATED)
- `reporting_date` - Date of the report
- `reporting_authority` - Authority code

### DebtInstrument

Extends Instrument with debt-specific fields:

**Core Debt Fields:**
- `total_issued_nominal_amount` - Total nominal amount issued
- `maturity_date` - Maturity date
- `nominal_value_per_unit` - Face value per unit
- `debt_seniority` - Seniority level (SNDB, SBOD, etc.)

**Interest Rate (Fixed):**
- `fixed_rate` - Fixed interest rate percentage

**Interest Rate (Floating):**
- `floating_rate_reference_isin` - Reference rate ISIN
- `floating_rate_reference_index` - Index name (EURIBOR, LIBOR, etc.)
- `floating_rate_reference_name` - Reference rate name
- `floating_rate_term_unit` - Term unit (MNTH, YEAR)
- `floating_rate_term_value` - Term value (integer)
- `floating_rate_basis_points` - Spread in basis points

**Properties:**
- `is_fixed_rate` - True if fixed rate instrument
- `is_floating_rate` - True if floating rate instrument

### EquityInstrument

Extends Instrument with equity-specific fields:

**Core Equity Fields:**
- `dividend_payment_frequency` - Frequency code
- `voting_rights_per_share` - Voting rights code (DVOT, DNVT, DVTX)
- `ownership_restriction` - Ownership restrictions
- `redemption_type` - Redemption type (REDF, NRDF)
- `capital_investment_restriction` - Investment restrictions

**Properties:**
- `has_voting_rights` - True if equity has voting rights
- `is_redeemable` - True if equity is redeemable

### DerivativeInstrument

Extends Instrument with derivative-specific fields:

**Core Derivative Fields:**
- `expiry_date` - Expiration/maturity date
- `price_multiplier` - Contract multiplier
- `underlying_isin` - ISIN of underlying instrument
- `underlying_index_name` - Name of underlying index
- `notional_currency_1` - First notional currency
- `notional_currency_2` - Second notional currency (for multi-currency)

**Commodity Derivatives:**
- `base_product` - Base product type
- `sub_product` - Sub-product type
- `further_sub_product` - Further classification
- `transaction_type` - Transaction type
- `final_price_type` - Final price determination

**Nested Derivative Attributes:**
- `option_attrs` - OptionAttributes dataclass (for options)
- `future_attrs` - FutureAttributes dataclass (for futures)

**Properties:**
- `is_option` - True if option (CFI code starts with 'I')
- `is_future` - True if future (CFI code starts with 'F')
- `is_swap` - True if swap (CFI code starts with 'S')
- `is_commodity_derivative` - True if commodity derivative indicator is 'true'

### OptionAttributes

Option-specific nested attributes:

- `option_type` - CALL or PUT
- `strike_price` - Strike price
- `strike_price_currency` - Strike price currency
- `option_style` - Exercise style (AMER, EURO, BERM, ASIA)
- `option_exercise_date` - Exercise date

### FutureAttributes

Future-specific nested attributes:

- `delivery_type` - PHYS (physical) or CASH (cash settled)
- `futures_value_date` - Value date
- `exchange_to_traded_for` - Exchange code

### TradingVenueAttributes

Trading venue information (nested in all instruments):

- `venue_id` - Trading venue identifier
- `issuer_request` - Boolean string
- `admission_approval_date` - Admission approval date
- `request_for_admission_date` - Request date
- `first_trade_date` - First trading date
- `termination_date` - Termination date

### TechnicalAttributes

Technical record attributes (nested in all instruments):

- `relevant_competent_authority` - Authority code
- `publication_period_from` - Publication start date
- `relevant_trading_venue` - Trading venue code
- `never_published` - Boolean

## Field Mapping

The `InstrumentMapper` automatically maps raw ESMA column names to clean Python properties:

| Raw Column Name | Python Property |
|----------------|-----------------|
| `RefData_FinInstrmGnlAttrbts_FullNm` | `full_name` |
| `RefData_FinInstrmGnlAttrbts_ClssfctnTp` | `classification_type` |
| `RefData_DebtInstrmAttrbts_MtrtyDt` | `maturity_date` |
| `RefData_DerivInstrmAttrbts_XpryDt` | `expiry_date` |
| `RefData_TradgVnRltdAttrbts_FrstTradDt` | `trading_venue.first_trade_date` |

The mapper handles:
- Column name normalization (with/without `RefData_` prefix)
- Type conversion (strings → dates, floats, ints)
- Null value handling
- Nested attribute creation
- Automatic model selection based on CFI code

## Examples

See the [examples](../examples/) directory for complete working examples:

- `normalize_firds_example.py` - Comprehensive example showing all model types

## CFI Code Reference

The first character of the CFI code determines the asset type and which model is used:

| Code | Type | Model |
|------|------|-------|
| C | Collective Investment | Instrument (base) |
| D | Debt | DebtInstrument |
| E | Equity | EquityInstrument |
| F | Futures | DerivativeInstrument |
| H | Other Derivative | DerivativeInstrument |
| I | Options | DerivativeInstrument |
| J | Forwards | DerivativeInstrument |
| O | Others | Instrument (base) |
| R | Referential | Instrument (base) |
| S | Swaps | DerivativeInstrument |

## Type Conversion

The mapper performs automatic type conversion:

- **Dates**: Parses ISO 8601, YYYY-MM-DD, YYYYMMDD formats
- **Floats**: Amounts, rates, multipliers
- **Integers**: Term values
- **Booleans**: String 'true'/'false' → Python bool
- **Strings**: Trimmed, null-safe

## Performance

The mapper is designed for efficiency:

- Uses pandas vectorization where possible
- Lazy attribute initialization
- Minimal memory overhead
- Processes 50,000 instruments in ~2-3 seconds

## Extension

To add custom fields or models:

1. Define new dataclass inheriting from appropriate base
2. Add field mappings to `InstrumentMapper.FIELD_MAP`
3. Create extraction method `_extract_your_fields()`
4. Update `from_row()` logic to instantiate your model

Example:

```python
from dataclasses import dataclass
from esma_dm.models import Instrument

@dataclass
class CustomInstrument(Instrument):
    custom_field: Optional[str] = None
    
    @property
    def custom_property(self) -> bool:
        return self.custom_field == 'CUSTOM'
```
