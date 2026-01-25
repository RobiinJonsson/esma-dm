#!/usr/bin/env python3
"""
ESMA Data Population Analysis Tool

Analyzes actual data population in ESMA CSV files to inform database schema
and bulk inserter design. Outputs detailed statistics about which columns
contain data for each asset type.

Usage:
    python tools/analyze_data_population.py
"""

import pandas as pd
from pathlib import Path
import sys
from collections import defaultdict
import re

def get_data_directories():
    """Get available data directories."""
    base_path = Path('downloads/data')
    if not base_path.exists():
        base_path = Path('esma_dm/data')  # Package-relative path
    
    dirs = []
    for subdir in base_path.iterdir():
        if subdir.is_dir() and list(subdir.glob('*.csv')):
            dirs.append(subdir)
    return dirs

def select_file_pattern():
    """Let user select file pattern to analyze."""
    patterns = {
        '1': ('FULINS', 'Full instrument files'),
        '2': ('DLTINS', 'Delta instrument files'), 
        '3': ('FULECR', 'Full ECR files'),
        '4': ('FULNCR', 'Full NCR files'),
        '5': ('ALL', 'All CSV files')
    }
    
    print("📋 SELECT FILE PATTERN TO ANALYZE")
    print("=" * 50)
    for key, (pattern, desc) in patterns.items():
        print(f"{key}. {pattern} - {desc}")
    
    while True:
        choice = input("\nEnter choice (1-5): ").strip()
        if choice in patterns:
            return patterns[choice][0]
        print("Invalid choice. Please select 1-5.")

def analyze_csv_file(file_path, max_rows=10000):
    """Analyze a single CSV file for data population."""
    try:
        # First check file size and adjust sample size
        file_size = file_path.stat().st_size / (1024 * 1024)  # MB
        if file_size > 50:  # For large files, use smaller sample
            sample_size = min(5000, max_rows)
        else:
            sample_size = max_rows
            
        print(f"  📄 {file_path.name} ({file_size:.1f}MB) - sampling {sample_size} rows...")
        
        # Load sample data
        df = pd.read_csv(file_path, nrows=sample_size, low_memory=False)
        
        if df.empty:
            return None
            
        # Get CFI column for asset type classification
        cfi_col = None
        for col in df.columns:
            if 'clssfctntp' in col.lower() or col.lower() in ['cfi_code', 'classification_type']:
                cfi_col = col
                break
                
        if not cfi_col:
            print(f"    ⚠️  No CFI column found, analyzing as mixed asset types")
            asset_types = ['UNKNOWN']
            df['_asset_type'] = 'UNKNOWN'
        else:
            # Extract asset types from CFI first character
            df['_asset_type'] = df[cfi_col].astype(str).str[0]
            asset_types = df['_asset_type'].value_counts().index.tolist()
            
        analysis_result = {
            'file_path': file_path,
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'asset_types': {},
            'file_size_mb': file_size
        }
        
        # Analyze each asset type separately
        for asset_type in asset_types:
            if asset_type in ['nan', '', None]:
                continue
                
            asset_df = df[df['_asset_type'] == asset_type] if cfi_col else df
            asset_count = len(asset_df)
            
            column_stats = {}
            for col in df.columns:
                if col == '_asset_type':
                    continue
                    
                series = asset_df[col]
                non_null_count = series.notna().sum()
                populated_pct = (non_null_count / asset_count * 100) if asset_count > 0 else 0
                
                # Get sample values (non-null)
                sample_values = series.dropna().head(3).tolist()
                
                column_stats[col] = {
                    'non_null_count': non_null_count,
                    'populated_percentage': populated_pct,
                    'sample_values': sample_values,
                    'total_rows': asset_count
                }
            
            analysis_result['asset_types'][asset_type] = {
                'count': asset_count,
                'columns': column_stats
            }
            
        return analysis_result
        
    except Exception as e:
        print(f"    ❌ Error analyzing {file_path.name}: {e}")
        return None

def save_analysis_report(analysis_data, pattern, output_dir):
    """Save comprehensive analysis report."""
    output_dir.mkdir(exist_ok=True)
    
    # Create summary report
    summary_file = output_dir / f"{pattern.lower()}_population_summary.txt"
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write(f"ESMA DATA POPULATION ANALYSIS - {pattern}\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {pd.Timestamp.now()}\n\n")
        
        # Overall statistics
        f.write("📊 OVERALL STATISTICS\n")
        f.write("-" * 40 + "\n")
        
        total_files = len(analysis_data)
        total_asset_types = set()
        total_rows = 0
        
        for analysis in analysis_data:
            if analysis:
                total_rows += analysis['total_rows']
                total_asset_types.update(analysis['asset_types'].keys())
        
        f.write(f"Files analyzed: {total_files}\n")
        f.write(f"Total rows: {total_rows:,}\n")
        f.write(f"Asset types found: {sorted(total_asset_types)}\n\n")
        
        # Per-file analysis
        for analysis in analysis_data:
            if not analysis:
                continue
                
            f.write("=" * 60 + "\n")
            f.write(f"FILE: {analysis['file_path'].name}\n")
            f.write("=" * 60 + "\n")
            f.write(f"Size: {analysis['file_size_mb']:.1f}MB\n")
            f.write(f"Rows: {analysis['total_rows']:,}\n")
            f.write(f"Columns: {analysis['total_columns']}\n\n")
            
            for asset_type, asset_data in analysis['asset_types'].items():
                f.write(f"🏷️  ASSET TYPE: {asset_type}\n")
                f.write(f"   Count: {asset_data['count']:,} instruments\n")
                f.write("-" * 50 + "\n")
                
                # Sort columns by population percentage
                columns_sorted = sorted(
                    asset_data['columns'].items(),
                    key=lambda x: x[1]['populated_percentage'],
                    reverse=True
                )
                
                f.write("Column                                    Populated    Non-Null  Sample Values\n")
                f.write("-" * 100 + "\n")
                
                for col_name, stats in columns_sorted:
                    pct = stats['populated_percentage']
                    count = stats['non_null_count']
                    samples = str(stats['sample_values'][:2])  # First 2 samples
                    
                    # Truncate long column names and samples
                    col_display = col_name[:40].ljust(40)
                    pct_display = f"{pct:6.1f}%".rjust(10)
                    count_display = f"{count:,}".rjust(8)
                    sample_display = samples[:45] + "..." if len(samples) > 45 else samples
                    
                    f.write(f"{col_display} {pct_display} {count_display}  {sample_display}\n")
                
                f.write("\n")
        
        # Create asset-specific detailed reports
        for asset_type in sorted(total_asset_types):
            if asset_type in ['UNKNOWN', 'nan']:
                continue
                
            asset_file = output_dir / f"{pattern.lower()}_asset_{asset_type}_details.txt"
            
            with open(asset_file, 'w', encoding='utf-8') as af:
                af.write("=" * 80 + "\n")
                af.write(f"ASSET TYPE {asset_type} - DETAILED ANALYSIS\n")
                af.write("=" * 80 + "\n\n")
                
                # Aggregate column statistics across all files for this asset type
                all_columns = {}
                total_instruments = 0
                
                for analysis in analysis_data:
                    if not analysis or asset_type not in analysis['asset_types']:
                        continue
                        
                    asset_data = analysis['asset_types'][asset_type]
                    total_instruments += asset_data['count']
                    
                    for col_name, stats in asset_data['columns'].items():
                        if col_name not in all_columns:
                            all_columns[col_name] = {
                                'files': [],
                                'total_non_null': 0,
                                'total_possible': 0,
                                'sample_values': set()
                            }
                        
                        all_columns[col_name]['files'].append(analysis['file_path'].name)
                        all_columns[col_name]['total_non_null'] += stats['non_null_count']
                        all_columns[col_name]['total_possible'] += stats['total_rows']
                        all_columns[col_name]['sample_values'].update(stats['sample_values'])
                
                af.write(f"Total {asset_type} instruments across all files: {total_instruments:,}\n\n")
                
                # Sort by overall population percentage
                columns_sorted = sorted(
                    all_columns.items(),
                    key=lambda x: (x[1]['total_non_null'] / max(1, x[1]['total_possible'])),
                    reverse=True
                )
                
                af.write("COLUMN POPULATION SUMMARY\n")
                af.write("-" * 80 + "\n")
                af.write("Column Name                               Overall%  Non-Null    Files  Sample Values\n")
                af.write("-" * 120 + "\n")
                
                for col_name, data in columns_sorted:
                    overall_pct = (data['total_non_null'] / max(1, data['total_possible'])) * 100
                    files_count = len(data['files'])
                    samples = list(data['sample_values'])[:3]
                    
                    col_display = col_name[:40].ljust(40)
                    pct_display = f"{overall_pct:6.1f}%".rjust(9)
                    count_display = f"{data['total_non_null']:,}".rjust(9)
                    files_display = str(files_count).rjust(5)
                    sample_display = str(samples)[:50] + "..." if len(str(samples)) > 50 else str(samples)
                    
                    af.write(f"{col_display} {pct_display} {count_display} {files_display}  {sample_display}\n")
    
    print(f"✅ Analysis reports saved to {output_dir}")
    print(f"   📄 Summary: {summary_file.name}")
    for asset_type in sorted(total_asset_types):
        if asset_type not in ['UNKNOWN', 'nan']:
            print(f"   📄 Asset {asset_type}: {pattern.lower()}_asset_{asset_type}_details.txt")

def main():
    print("🔍 ESMA DATA POPULATION ANALYSIS TOOL")
    print("=" * 50)
    
    # Get available data directories
    data_dirs = get_data_directories()
    if not data_dirs:
        print("❌ No data directories with CSV files found.")
        print("   Expected: downloads/data/ or esma_dm/data/")
        return
    
    print(f"📁 Found data directories: {[d.name for d in data_dirs]}")
    
    # Select file pattern
    pattern = select_file_pattern()
    print(f"\n🎯 Analyzing files matching: {pattern}")
    
    # Collect all matching files
    all_files = []
    for data_dir in data_dirs:
        if pattern == 'ALL':
            files = list(data_dir.glob('*.csv'))
        else:
            files = list(data_dir.glob(f'{pattern}*.csv'))
        all_files.extend(files)
    
    if not all_files:
        print(f"❌ No CSV files found matching pattern '{pattern}'")
        return
    
    print(f"📊 Found {len(all_files)} files to analyze")
    
    # Analyze each file
    analysis_results = []
    for i, file_path in enumerate(all_files, 1):
        print(f"\n[{i}/{len(all_files)}] Analyzing {file_path.parent.name}/{file_path.name}")
        result = analyze_csv_file(file_path)
        if result:
            analysis_results.append(result)
    
    if not analysis_results:
        print("❌ No files could be analyzed successfully")
        return
    
    # Create output directory
    output_dir = Path('downloads/data/analysis')
    if not output_dir.parent.exists():
        output_dir = Path('esma_dm/data/analysis')
    
    # Save comprehensive report
    save_analysis_report(analysis_results, pattern, output_dir)
    
    print(f"\n🎉 Analysis complete! Check {output_dir} for detailed reports.")

if __name__ == '__main__':
    main()