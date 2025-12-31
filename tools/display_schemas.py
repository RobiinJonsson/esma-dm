"""
Display data schemas for all ESMA instrument models.

This script demonstrates the schema introspection capabilities of the
normalized data models, showing all fields, types, and descriptions.
"""
from esma_dm.models import (
    Instrument,
    DebtInstrument,
    EquityInstrument,
    DerivativeInstrument,
    OptionAttributes,
    FutureAttributes,
    TradingVenueAttributes,
    TechnicalAttributes,
)


def print_schema(name: str, schema: dict, indent: int = 0):
    """Print a schema in a formatted way."""
    prefix = "  " * indent
    
    print(f"\n{prefix}{'='*60}")
    print(f"{prefix}{name}")
    print(f"{prefix}{'='*60}")
    
    # Sort fields by required first, then alphabetically
    sorted_fields = sorted(
        schema.items(),
        key=lambda x: (not x[1].get('required', False), x[0])
    )
    
    for field_name, field_info in sorted_fields:
        field_type = field_info.get('type', 'unknown')
        description = field_info.get('description', 'No description')
        required = field_info.get('required', False)
        req_marker = " [REQUIRED]" if required else ""
        
        print(f"\n{prefix}  {field_name}{req_marker}")
        print(f"{prefix}    Type: {field_type}")
        print(f"{prefix}    Description: {description}")


def compare_schemas(base_name: str, base_schema: dict, derived_name: str, derived_schema: dict):
    """Compare two schemas and show what's added."""
    base_fields = set(base_schema.keys())
    derived_fields = set(derived_schema.keys())
    
    added_fields = derived_fields - base_fields
    
    if added_fields:
        print(f"\n  Additional fields in {derived_name} (vs {base_name}):")
        for field in sorted(added_fields):
            field_type = derived_schema[field].get('type', 'unknown')
            print(f"    + {field} ({field_type})")
    
    print(f"\n  Total fields: {len(base_fields)} base + {len(added_fields)} specific = {len(derived_fields)} total")


def main():
    """Main entry point."""
    print("="*80)
    print("ESMA DATA MODEL SCHEMAS")
    print("="*80)
    print("\nThis document shows all available fields in each instrument model,")
    print("including types, descriptions, and required fields.")
    
    # Base/Common Attributes
    print("\n\n" + "="*80)
    print("COMMON ATTRIBUTE CLASSES")
    print("="*80)
    
    print_schema("TradingVenueAttributes", TradingVenueAttributes.get_schema(), indent=0)
    print_schema("TechnicalAttributes", TechnicalAttributes.get_schema(), indent=0)
    
    # Base Instrument
    print("\n\n" + "="*80)
    print("BASE INSTRUMENT MODEL")
    print("="*80)
    print("\nUsed for asset types: C (Collective Investment), O (Others), R (Referential)")
    
    base_schema = Instrument.get_schema()
    print_schema("Instrument (Base Class)", base_schema, indent=0)
    
    # Debt Instrument
    print("\n\n" + "="*80)
    print("DEBT INSTRUMENT MODEL")
    print("="*80)
    print("\nUsed for asset type: D (Debt instruments - bonds, notes, etc.)")
    
    debt_schema = DebtInstrument.get_schema()
    print_schema("DebtInstrument", debt_schema, indent=0)
    compare_schemas("Instrument", base_schema, "DebtInstrument", debt_schema)
    
    # Equity Instrument
    print("\n\n" + "="*80)
    print("EQUITY INSTRUMENT MODEL")
    print("="*80)
    print("\nUsed for asset type: E (Equities - shares, stocks, etc.)")
    
    equity_schema = EquityInstrument.get_schema()
    print_schema("EquityInstrument", equity_schema, indent=0)
    compare_schemas("Instrument", base_schema, "EquityInstrument", equity_schema)
    
    # Derivative Instrument
    print("\n\n" + "="*80)
    print("DERIVATIVE INSTRUMENT MODEL")
    print("="*80)
    print("\nUsed for asset types: F (Futures), I (Options), J (Forwards), S (Swaps), H (Others)")
    
    derivative_schema = DerivativeInstrument.get_schema()
    print_schema("DerivativeInstrument", derivative_schema, indent=0)
    compare_schemas("Instrument", base_schema, "DerivativeInstrument", derivative_schema)
    
    # Nested derivative attributes
    print("\n" + "-"*80)
    print("Nested Derivative Attributes")
    print("-"*80)
    
    print_schema("OptionAttributes (nested in DerivativeInstrument)", OptionAttributes.get_schema(), indent=1)
    print_schema("FutureAttributes (nested in DerivativeInstrument)", FutureAttributes.get_schema(), indent=1)
    
    # Summary
    print("\n\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    
    print(f"\nBase Instrument: {len(base_schema)} fields")
    print(f"DebtInstrument: {len(debt_schema)} fields ({len(debt_schema) - len(base_schema)} debt-specific)")
    print(f"EquityInstrument: {len(equity_schema)} fields ({len(equity_schema) - len(base_schema)} equity-specific)")
    print(f"DerivativeInstrument: {len(derivative_schema)} fields ({len(derivative_schema) - len(base_schema)} derivative-specific)")
    
    print("\nNested attribute classes:")
    print(f"  TradingVenueAttributes: {len(TradingVenueAttributes.get_schema())} fields")
    print(f"  TechnicalAttributes: {len(TechnicalAttributes.get_schema())} fields")
    print(f"  OptionAttributes: {len(OptionAttributes.get_schema())} fields")
    print(f"  FutureAttributes: {len(FutureAttributes.get_schema())} fields")
    
    # Asset type mapping
    print("\n" + "="*80)
    print("ASSET TYPE TO MODEL MAPPING")
    print("="*80)
    
    mapping = [
        ("C", "Collective Investment", "Instrument (base)"),
        ("D", "Debt", "DebtInstrument"),
        ("E", "Equity", "EquityInstrument"),
        ("F", "Futures", "DerivativeInstrument"),
        ("H", "Other Derivative", "DerivativeInstrument"),
        ("I", "Options", "DerivativeInstrument"),
        ("J", "Forwards", "DerivativeInstrument"),
        ("O", "Others", "Instrument (base)"),
        ("R", "Referential", "Instrument (base)"),
        ("S", "Swaps", "DerivativeInstrument"),
    ]
    
    print("\n{:<8} {:<25} {:<30}".format("CFI", "Asset Type", "Model Class"))
    print("-" * 80)
    for cfi, asset_type, model in mapping:
        print("{:<8} {:<25} {:<30}".format(cfi, asset_type, model))
    
    print("\n" + "="*80)
    print("USAGE EXAMPLE")
    print("="*80)
    print("""
# Get schema for any model class:
from esma_dm.models import DebtInstrument

schema = DebtInstrument.get_schema()

# Iterate over fields:
for field_name, field_info in schema.items():
    print(f"{field_name}: {field_info['type']} - {field_info['description']}")

# Check if field is required:
if schema['isin'].get('required', False):
    print("ISIN is required!")
""")


if __name__ == '__main__':
    main()
