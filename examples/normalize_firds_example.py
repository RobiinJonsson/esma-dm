"""
Example script demonstrating the use of normalized data models.

This script shows how to:
1. Load raw FIRDS data from an inspection CSV
2. Convert to normalized instrument models using InstrumentMapper
3. Access data through clean Python properties
4. Filter and analyze by instrument type
"""
import pandas as pd
from pathlib import Path
from esma_dm.models import InstrumentMapper, DebtInstrument, EquityInstrument, DerivativeInstrument


def load_and_normalize_firds_data(csv_path: Path):
    """Load raw FIRDS data and convert to normalized models."""
    print(f"Loading raw data from {csv_path}...")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} records with {len(df.columns)} columns")
    
    print("\nConverting to normalized models...")
    instruments = InstrumentMapper.from_dataframe(df)
    print(f"Created {len(instruments):,} instrument objects")
    
    return instruments


def analyze_instruments(instruments: list):
    """Analyze and display statistics about instruments."""
    # Count by type
    type_counts = {}
    for inst in instruments:
        inst_type = type(inst).__name__
        type_counts[inst_type] = type_counts.get(inst_type, 0) + 1
    
    print("\n" + "="*60)
    print("INSTRUMENT TYPE DISTRIBUTION")
    print("="*60)
    for inst_type, count in sorted(type_counts.items()):
        print(f"{inst_type:30} {count:>10,}")
    
    # Count by asset type (CFI first character)
    asset_counts = {}
    for inst in instruments:
        if inst.asset_type:
            asset_counts[inst.asset_type] = asset_counts.get(inst.asset_type, 0) + 1
    
    print("\n" + "="*60)
    print("ASSET TYPE DISTRIBUTION (CFI Code)")
    print("="*60)
    asset_names = {
        'C': 'Collective Investment',
        'D': 'Debt',
        'E': 'Equity',
        'F': 'Futures',
        'H': 'Other Derivative',
        'I': 'Options',
        'J': 'Forwards',
        'O': 'Others',
        'R': 'Referential',
        'S': 'Swaps',
    }
    for asset_type, count in sorted(asset_counts.items()):
        name = asset_names.get(asset_type, 'Unknown')
        print(f"{asset_type} - {name:30} {count:>10,}")


def show_debt_examples(instruments: list, count: int = 3):
    """Show examples of debt instruments with normalized fields."""
    debt_instruments = [i for i in instruments if isinstance(i, DebtInstrument)]
    
    if not debt_instruments:
        print("\nNo debt instruments found")
        return
    
    print("\n" + "="*60)
    print(f"DEBT INSTRUMENT EXAMPLES ({len(debt_instruments):,} total)")
    print("="*60)
    
    for idx, debt in enumerate(debt_instruments[:count], 1):
        print(f"\n{idx}. {debt.full_name or 'N/A'}")
        print(f"   ISIN: {debt.isin}")
        print(f"   Currency: {debt.notional_currency}")
        print(f"   Maturity Date: {debt.maturity_date}")
        print(f"   Total Issued: {debt.total_issued_nominal_amount:,.2f}" if debt.total_issued_nominal_amount else "   Total Issued: N/A")
        print(f"   Nominal Value/Unit: {debt.nominal_value_per_unit:,.2f}" if debt.nominal_value_per_unit else "   Nominal Value/Unit: N/A")
        
        if debt.is_fixed_rate:
            print(f"   Interest Rate: Fixed {debt.fixed_rate}%")
        elif debt.is_floating_rate:
            ref = debt.floating_rate_reference_index or debt.floating_rate_reference_name
            print(f"   Interest Rate: Floating (ref: {ref})")
            if debt.floating_rate_basis_points:
                print(f"   Spread: {debt.floating_rate_basis_points} bps")
        
        if debt.debt_seniority:
            print(f"   Seniority: {debt.debt_seniority}")


def show_equity_examples(instruments: list, count: int = 3):
    """Show examples of equity instruments with normalized fields."""
    equity_instruments = [i for i in instruments if isinstance(i, EquityInstrument)]
    
    if not equity_instruments:
        print("\nNo equity instruments found")
        return
    
    print("\n" + "="*60)
    print(f"EQUITY INSTRUMENT EXAMPLES ({len(equity_instruments):,} total)")
    print("="*60)
    
    for idx, equity in enumerate(equity_instruments[:count], 1):
        print(f"\n{idx}. {equity.full_name or 'N/A'}")
        print(f"   ISIN: {equity.isin}")
        print(f"   Currency: {equity.notional_currency}")
        print(f"   Voting Rights: {'Yes' if equity.has_voting_rights else 'No/Unknown'}")
        print(f"   Redeemable: {'Yes' if equity.is_redeemable else 'No/Unknown'}")
        
        if equity.dividend_payment_frequency:
            print(f"   Dividend Frequency: {equity.dividend_payment_frequency}")
        
        if equity.trading_venue and equity.trading_venue.first_trade_date:
            print(f"   First Trade Date: {equity.trading_venue.first_trade_date}")


def show_derivative_examples(instruments: list, count: int = 3):
    """Show examples of derivative instruments with normalized fields."""
    derivative_instruments = [i for i in instruments if isinstance(i, DerivativeInstrument)]
    
    if not derivative_instruments:
        print("\nNo derivative instruments found")
        return
    
    print("\n" + "="*60)
    print(f"DERIVATIVE INSTRUMENT EXAMPLES ({len(derivative_instruments):,} total)")
    print("="*60)
    
    for idx, deriv in enumerate(derivative_instruments[:count], 1):
        print(f"\n{idx}. {deriv.full_name or 'N/A'}")
        print(f"   ISIN: {deriv.isin}")
        print(f"   Asset Type: ", end="")
        if deriv.is_option:
            print("Option")
        elif deriv.is_future:
            print("Future")
        elif deriv.is_swap:
            print("Swap")
        else:
            print("Other Derivative")
        
        print(f"   Expiry Date: {deriv.expiry_date}")
        print(f"   Underlying ISIN: {deriv.underlying_isin or 'N/A'}")
        
        if deriv.is_commodity_derivative:
            print(f"   Commodity Derivative: Yes")
            print(f"   Base Product: {deriv.base_product or 'N/A'}")
            print(f"   Sub Product: {deriv.sub_product or 'N/A'}")
        
        if deriv.option_attrs:
            print(f"   Option Type: {deriv.option_attrs.option_type}")
            print(f"   Strike Price: {deriv.option_attrs.strike_price} {deriv.option_attrs.strike_price_currency or ''}")
            print(f"   Option Style: {deriv.option_attrs.option_style}")
        
        if deriv.future_attrs:
            print(f"   Delivery Type: {deriv.future_attrs.delivery_type}")
            print(f"   Value Date: {deriv.future_attrs.futures_value_date}")
        
        if deriv.price_multiplier:
            print(f"   Price Multiplier: {deriv.price_multiplier}")


def main():
    """Main entry point."""
    # Example usage - adjust path to your inspection output
    downloads_dir = Path(__file__).parent.parent / 'downloads' / 'data' / 'firds'
    
    # Find FULINS inspection files (they have more data than DLTINS samples)
    inspection_files = list(downloads_dir.glob('FULINS_*_inspection.csv'))
    
    if not inspection_files:
        print("No inspection files found")
        print(f"Looked in: {downloads_dir}")
        print("\nRun the inspect_firds_files.py tool first to generate data")
        print("Example: python tools/inspect_firds_files.py --asset-type F --date 2024-06-01")
        return
    
    # Use the most recent file
    latest_file = max(inspection_files, key=lambda p: p.stat().st_mtime)
    
    print("="*60)
    print("ESMA FIRDS DATA NORMALIZATION EXAMPLE")
    print("="*60)
    print(f"\nUsing file: {latest_file.name}")
    
    # Load and normalize
    instruments = load_and_normalize_firds_data(latest_file)
    
    # Analyze
    analyze_instruments(instruments)
    
    # Show examples
    show_debt_examples(instruments)
    show_equity_examples(instruments)
    show_derivative_examples(instruments)
    
    print("\n" + "="*60)
    print("NORMALIZED MODEL BENEFITS")
    print("="*60)
    print("+ Clean Python property access (no raw column names)")
    print("+ Type-specific models (DebtInstrument, EquityInstrument, etc.)")
    print("+ Type conversion (dates, floats, strings)")
    print("+ Computed properties (is_fixed_rate, has_voting_rights, etc.)")
    print("+ Nested attributes for complex data")
    print("+ Easy filtering and analysis by instrument type")


if __name__ == '__main__':
    main()
