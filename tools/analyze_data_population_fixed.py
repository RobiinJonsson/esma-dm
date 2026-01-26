#!/usr/bin/env python3
"""
ESMA Data Population Analysis Tool - FIXED VERSION

Analyzes actual data population in ESMA CSV files to inform database schema
and bulk inserter design. Outputs detailed statistics about which columns
contain data for each asset type.

FIXES:
- Captures full column names (no truncation)
- Creates JSON output for programmatic use  
- Better column mapping analysis
- Asset-specific column usage patterns

Usage:
    python tools/analyze_data_population_fixed.py
"""

import pandas as pd
import json
from pathlib import Path
import sys
from collections import defaultdict
import re
from datetime import datetime

def get_data_directories():
    """Get available data directories."""
    base_path = Path('downloads/data')
    if not base_path.exists():
        base_path = Path('esma_dm/data')
    
    dirs = []
    for subdir in base_path.iterdir():
        if subdir.is_dir() and list(subdir.glob('*.csv')):
            dirs.append(subdir)
    return dirs

def select_analysis_scope():
    """Select what to analyze."""
    print("📋 SELECT ANALYSIS SCOPE")
    print("=" * 50)
    print("1. FULINS - Full instrument files (recommended)")
    print("2. DLTINS - Delta instrument files") 
    print("3. All CSV files")
    
    while True:
        choice = input("\nEnter choice (1-3): ").strip()
        if choice == '1':
            return 'FULINS'
        elif choice == '2':
            return 'DLTINS'
        elif choice == '3':
            return 'ALL'
        print("Invalid choice. Please select 1-3.")

def extract_asset_type_from_cfi(cfi_code):
    """Extract asset type from CFI code first character."""
    if pd.isna(cfi_code) or not str(cfi_code):
        return 'unknown'
    return str(cfi_code)[0].upper()

def analyze_csv_file(file_path, max_rows=10000):
    """Analyze a single CSV file comprehensively."""
    try:
        print(f"    📊 Analyzing {file_path.name}...")
        
        # Read file with proper settings
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False, nrows=max_rows)
        
        if len(df) == 0:
            print(f"    ⚠️  Empty file: {file_path.name}")
            return None
        
        # Find CFI column (various possible names)
        cfi_column = None
        for col in df.columns:
            if any(pattern in col.lower() for pattern in ['clssfctn', 'cfi', 'classification']):
                cfi_column = col
                break
        
        if not cfi_column:
            print(f"    ⚠️  No CFI column found in {file_path.name}")
            return None
        
        # Add asset type based on CFI
        df['_asset_type'] = df[cfi_column].apply(extract_asset_type_from_cfi)
        
        # Basic file info
        analysis_result = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_size_mb': file_path.stat().st_size / 1024 / 1024,
            'total_rows': len(df),
            'total_columns': len(df.columns) - 1,  # Exclude _asset_type
            'cfi_column_used': cfi_column,
            'columns_list': [col for col in df.columns if col != '_asset_type'],
            'asset_types_found': {},
            'column_analysis': {}
        }
        
        # Analyze by asset type
        asset_types = df['_asset_type'].value_counts()
        
        for asset_type in asset_types.index:
            if pd.isna(asset_type):
                continue
                
            asset_df = df[df['_asset_type'] == asset_type]
            asset_count = len(asset_df)
            
            # Column-by-column analysis for this asset type
            column_stats = {}
            for col in df.columns:
                if col == '_asset_type':
                    continue
                
                series = asset_df[col]
                non_null_count = series.notna().sum()
                populated_pct = (non_null_count / asset_count * 100) if asset_count > 0 else 0
                
                # Get sample values (limit to reasonable size)
                sample_values = []
                for val in series.dropna().head(5):
                    val_str = str(val)
                    if len(val_str) > 100:
                        val_str = val_str[:100] + "..."
                    sample_values.append(val_str)
                
                # Data type analysis
                dtype_info = str(series.dtype)
                unique_count = series.nunique()
                
                column_stats[col] = {
                    'full_column_name': col,  # Preserve full name
                    'non_null_count': int(non_null_count),
                    'populated_percentage': round(populated_pct, 2),
                    'sample_values': sample_values,
                    'data_type': dtype_info,
                    'unique_values': int(unique_count),
                    'total_rows': int(asset_count)
                }
            
            analysis_result['asset_types_found'][asset_type] = {
                'count': int(asset_count),
                'percentage_of_file': round((asset_count / len(df)) * 100, 2),
                'columns': column_stats
            }
        
        # Overall column usage (across all asset types)
        overall_column_stats = {}
        for col in df.columns:
            if col == '_asset_type':
                continue
                
            series = df[col]
            non_null_count = series.notna().sum()
            populated_pct = (non_null_count / len(df) * 100) if len(df) > 0 else 0
            
            overall_column_stats[col] = {
                'full_column_name': col,
                'non_null_count': int(non_null_count),
                'populated_percentage': round(populated_pct, 2),
                'data_type': str(series.dtype),
                'unique_values': int(series.nunique())
            }
        
        analysis_result['column_analysis'] = overall_column_stats
        
        return analysis_result
        
    except Exception as e:
        print(f"    ❌ Error analyzing {file_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_column_mapping_guide(analysis_data, output_dir):
    """Create a comprehensive column mapping guide for all asset types."""
    
    # Collect all columns by asset type
    asset_columns = defaultdict(set)
    column_usage_by_asset = defaultdict(dict)
    
    for analysis in analysis_data:
        if not analysis:
            continue
            
        for asset_type, asset_data in analysis['asset_types_found'].items():
            for col_name, col_stats in asset_data['columns'].items():
                asset_columns[asset_type].add(col_name)
                
                # Track usage statistics
                if col_name not in column_usage_by_asset[asset_type]:
                    column_usage_by_asset[asset_type][col_name] = {
                        'total_appearances': 0,
                        'total_populated': 0,
                        'max_population_pct': 0,
                        'sample_values': []
                    }
                
                usage = column_usage_by_asset[asset_type][col_name]
                usage['total_appearances'] += 1
                usage['total_populated'] += col_stats['non_null_count']
                usage['max_population_pct'] = max(usage['max_population_pct'], col_stats['populated_percentage'])
                usage['sample_values'].extend(col_stats['sample_values'])
                # Keep only unique samples, limited count
                usage['sample_values'] = list(set(usage['sample_values']))[:10]
    
    # Create mapping guide
    mapping_guide = {
        'generated_at': datetime.now().isoformat(),
        'description': 'FIRDS Column Mapping Guide - Full column names by asset type',
        'asset_types': {}
    }
    
    for asset_type in sorted(asset_columns.keys()):
        columns = asset_columns[asset_type]
        usage_data = column_usage_by_asset[asset_type]
        
        # Categorize columns
        common_columns = []
        derivative_columns = []
        asset_specific_columns = []
        trading_venue_columns = []
        technical_columns = []
        
        for col in columns:
            col_lower = col.lower()
            usage = usage_data[col]
            
            col_info = {
                'column_name': col,
                'max_population_percentage': usage['max_population_pct'],
                'total_files_with_data': usage['total_appearances'],
                'sample_values': usage['sample_values'][:3]  # Top 3 samples
            }
            
            if 'derivinstrmattrbts' in col_lower:
                derivative_columns.append(col_info)
            elif 'tradgvnrltdattrbts' in col_lower:
                trading_venue_columns.append(col_info)
            elif 'techattrbts' in col_lower:
                technical_columns.append(col_info)
            elif any(pattern in col_lower for pattern in ['isin', 'fullnm', 'shrtnm', 'clssfctn', 'ntnlccy', 'issr']):
                common_columns.append(col_info)
            else:
                asset_specific_columns.append(col_info)
        
        mapping_guide['asset_types'][asset_type] = {
            'total_unique_columns': len(columns),
            'column_categories': {
                'common_instrument_fields': sorted(common_columns, key=lambda x: x['max_population_percentage'], reverse=True),
                'derivative_fields': sorted(derivative_columns, key=lambda x: x['max_population_percentage'], reverse=True),
                'trading_venue_fields': sorted(trading_venue_columns, key=lambda x: x['max_population_percentage'], reverse=True),
                'technical_fields': sorted(technical_columns, key=lambda x: x['max_population_percentage'], reverse=True),
                'asset_specific_fields': sorted(asset_specific_columns, key=lambda x: x['max_population_percentage'], reverse=True)
            }
        }
    
    # Save as JSON
    mapping_file = output_dir / 'firds_column_mapping_guide.json'
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(mapping_guide, f, indent=2, ensure_ascii=False)
    
    # Create human-readable summary
    summary_file = output_dir / 'firds_column_mapping_summary.txt'
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("FIRDS COLUMN MAPPING GUIDE\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now()}\n\n")
        
        for asset_type, data in mapping_guide['asset_types'].items():
            f.write(f"\nASSET TYPE: {asset_type}\n")
            f.write("=" * 40 + "\n")
            f.write(f"Total unique columns: {data['total_unique_columns']}\n\n")
            
            for category, columns in data['column_categories'].items():
                if columns:
                    f.write(f"{category.upper().replace('_', ' ')}:\n")
                    f.write("-" * 60 + "\n")
                    for col_info in columns[:10]:  # Top 10 per category
                        f.write(f"  {col_info['column_name']}\n")
                        f.write(f"    Population: {col_info['max_population_percentage']:.1f}%\n")
                        if col_info['sample_values']:
                            f.write(f"    Samples: {col_info['sample_values']}\n")
                        f.write("\n")
    
    print(f"📋 Column mapping guide saved to {mapping_file}")
    print(f"📋 Human-readable summary saved to {summary_file}")

def main():
    """Main analysis function."""
    print("🔍 ESMA DATA POPULATION ANALYSIS - FIXED VERSION")
    print("=" * 60)
    
    # Get data directories
    data_dirs = get_data_directories()
    if not data_dirs:
        print("❌ No data directories found. Run download first.")
        return
    
    print(f"📂 Found data in: {[str(d) for d in data_dirs]}")
    
    # Select pattern
    pattern = select_analysis_scope()
    print(f"🎯 Analyzing pattern: {pattern}")
    
    # Create output directory
    output_dir = Path('downloads/analysis')
    output_dir.mkdir(exist_ok=True)
    
    # Analyze all matching files
    all_analysis_data = []
    
    for data_dir in data_dirs:
        if pattern == 'ALL':
            csv_files = list(data_dir.glob('*.csv'))
        else:
            csv_files = list(data_dir.glob(f'{pattern}_*.csv'))
        
        if not csv_files:
            print(f"⚠️  No {pattern} files found in {data_dir}")
            continue
        
        print(f"\n📁 Processing {len(csv_files)} files from {data_dir.name}")
        
        for csv_file in csv_files[:5]:  # Limit to 5 files per directory for now
            analysis_result = analyze_csv_file(csv_file)
            if analysis_result:
                all_analysis_data.append(analysis_result)
    
    if not all_analysis_data:
        print("❌ No valid analysis data collected")
        return
    
    # Save comprehensive analysis
    output_file = output_dir / f'{pattern.lower()}_comprehensive_analysis.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_analysis_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Comprehensive analysis saved to {output_file}")
    
    # Create column mapping guide
    create_column_mapping_guide(all_analysis_data, output_dir)
    
    print("\n🎉 Analysis complete! Check downloads/analysis/ for results.")

if __name__ == "__main__":
    main()