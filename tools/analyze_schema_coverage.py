"""
Analyze FIRDS data files and show field coverage by asset type.

This script loads actual FIRDS data and compares the raw columns
to the normalized model schema, showing coverage statistics.
"""
import pandas as pd
from pathlib import Path
from collections import defaultdict
from esma_dm.models import (
    Instrument,
    DebtInstrument,
    EquityInstrument,
    DerivativeInstrument,
    InstrumentMapper,
)


def get_model_for_cfi(cfi_code: str):
    """Get the appropriate model class for a CFI code."""
    if not cfi_code or len(cfi_code) == 0:
        return Instrument
    
    asset_type = cfi_code[0]
    
    if asset_type == 'D':
        return DebtInstrument
    elif asset_type == 'E':
        return EquityInstrument
    elif asset_type in ('F', 'I', 'J', 'S', 'H'):
        return DerivativeInstrument
    else:
        return Instrument


def analyze_file(file_path: Path, sample_size: int = 1000):
    """Analyze a FIRDS file and return schema coverage information."""
    print(f"\nAnalyzing: {file_path.name}")
    
    # Load sample
    df = pd.read_csv(file_path, nrows=sample_size, low_memory=False)
    
    # Determine asset type from filename or data
    asset_type_from_file = file_path.name.split('_')[1] if len(file_path.name.split('_')) > 1 else 'Unknown'
    
    # Get CFI codes
    cfi_col = 'RefData_FinInstrmGnlAttrbts_ClssfctnTp'
    if cfi_col not in df.columns:
        cfi_col = 'FinInstrmGnlAttrbts_ClssfctnTp'
    
    if cfi_col in df.columns:
        cfis = df[cfi_col].dropna().unique()
        asset_types = set([cfi[0] for cfi in cfis if len(str(cfi)) > 0])
    else:
        asset_types = {asset_type_from_file}
    
    # Determine model class
    if len(asset_types) == 1:
        model_class = get_model_for_cfi(list(asset_types)[0])
    else:
        model_class = Instrument  # Mixed types, use base
    
    # Get model schema
    schema = model_class.get_schema()
    schema_fields = set(schema.keys())
    
    # Get raw columns (normalized)
    raw_columns = set(df.columns)
    
    # Normalize column names by removing RefData_ prefix
    normalized_raw = set()
    for col in raw_columns:
        if col.startswith('RefData_'):
            normalized_raw.add(col[8:])
        else:
            normalized_raw.add(col)
    
    # Count non-null values for each column
    non_null_counts = {}
    for col in df.columns:
        non_null = df[col].notna().sum()
        if non_null > 0:
            non_null_counts[col] = non_null
    
    return {
        'file': file_path.name,
        'asset_type': asset_type_from_file,
        'asset_types_in_data': sorted(asset_types),
        'model_class': model_class.__name__,
        'records': len(df),
        'total_columns': len(raw_columns),
        'columns_with_data': len(non_null_counts),
        'schema_fields': len(schema_fields),
        'raw_columns': raw_columns,
        'non_null_counts': non_null_counts,
        'model_schema': schema,
    }


def print_analysis(analysis: dict):
    """Print analysis results."""
    print(f"  Asset Type: {analysis['asset_type']} ({', '.join(analysis['asset_types_in_data'])})")
    print(f"  Model Class: {analysis['model_class']}")
    print(f"  Records Analyzed: {analysis['records']:,}")
    print(f"  Total Columns: {analysis['total_columns']}")
    print(f"  Columns with Data: {analysis['columns_with_data']}")
    print(f"  Model Schema Fields: {analysis['schema_fields']}")
    
    # Show top populated columns
    sorted_cols = sorted(
        analysis['non_null_counts'].items(),
        key=lambda x: x[1],
        reverse=True
    )
    
    print(f"\n  Top 10 Most Populated Columns:")
    for col, count in sorted_cols[:10]:
        pct = (count / analysis['records']) * 100
        print(f"    {col:50} {count:>6,} ({pct:>5.1f}%)")


def main():
    """Main entry point."""
    data_dir = Path('downloads/data/firds')
    
    print("="*80)
    print("FIRDS DATA SCHEMA ANALYSIS")
    print("="*80)
    print("\nAnalyzing actual FIRDS data files and comparing to model schemas")
    
    # Find all FULINS data files
    data_files = sorted(data_dir.glob('FULINS_*_data.csv'))
    
    if not data_files:
        print(f"\nNo data files found in {data_dir}")
        return
    
    print(f"\nFound {len(data_files)} data files")
    
    # Analyze each file
    results = []
    for file_path in data_files:
        try:
            analysis = analyze_file(file_path, sample_size=1000)
            results.append(analysis)
            print_analysis(analysis)
        except Exception as e:
            print(f"  Error analyzing {file_path.name}: {e}")
    
    # Summary by asset type
    print("\n\n" + "="*80)
    print("SUMMARY BY ASSET TYPE")
    print("="*80)
    
    asset_summary = defaultdict(lambda: {
        'files': 0,
        'total_columns': set(),
        'model_class': None,
    })
    
    for result in results:
        asset_type = result['asset_type']
        asset_summary[asset_type]['files'] += 1
        asset_summary[asset_type]['total_columns'].update(result['raw_columns'])
        asset_summary[asset_type]['model_class'] = result['model_class']
    
    print("\n{:<12} {:<25} {:<8} {:<15}".format(
        "Asset Type", "Model Class", "Files", "Unique Columns"
    ))
    print("-" * 80)
    
    for asset_type in sorted(asset_summary.keys()):
        info = asset_summary[asset_type]
        print("{:<12} {:<25} {:<8} {:<15}".format(
            asset_type,
            info['model_class'] or 'N/A',
            info['files'],
            len(info['total_columns'])
        ))
    
    # Overall statistics
    print("\n\n" + "="*80)
    print("OVERALL STATISTICS")
    print("="*80)
    
    total_files = len(results)
    total_records = sum(r['records'] for r in results)
    
    print(f"\nTotal Files Analyzed: {total_files}")
    print(f"Total Records Analyzed: {total_records:,}")
    
    # Count by model class
    model_counts = defaultdict(int)
    for result in results:
        model_counts[result['model_class']] += 1
    
    print("\nFiles by Model Class:")
    for model, count in sorted(model_counts.items()):
        print(f"  {model:30} {count:>3} files")
    
    # Show which models cover which asset types
    print("\n\n" + "="*80)
    print("MODEL COVERAGE")
    print("="*80)
    
    coverage = defaultdict(set)
    for result in results:
        coverage[result['model_class']].add(result['asset_type'])
    
    for model, asset_types in sorted(coverage.items()):
        print(f"\n{model}:")
        print(f"  Covers asset types: {', '.join(sorted(asset_types))}")


if __name__ == '__main__':
    main()
