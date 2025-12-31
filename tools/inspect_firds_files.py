"""
FIRDS File Inspector

A utility script to download, parse, and inspect FIRDS files to understand their structure.
This helps in building proper data models for different instrument types.

Usage:
    python tools/inspect_firds_files.py --file-type FULINS --asset-type E --date 20241220
    python tools/inspect_firds_files.py --file-type DLTINS --date 20241220
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import esma_dm
sys.path.insert(0, str(Path(__file__).parent.parent))

from esma_dm import FIRDSClient, Config
import pandas as pd


class FIRDSFileInspector:
    """Utility to inspect FIRDS files and understand their structure."""
    
    def __init__(self, output_dir: str = "downloads/inspections", date: str = None):
        """
        Initialize the inspector.
        
        Args:
            output_dir: Directory to save inspection results
            date: Optional date in YYYYMMDD format to narrow the search range
        """
        # If specific date provided, search within a narrow window around it
        # Otherwise use broad range from 2020
        if date and len(date) == 8:
            # Convert YYYYMMDD to YYYY-MM-DD and create 30-day window
            year = date[:4]
            month = date[4:6]
            day = date[6:8]
            from datetime import datetime, timedelta
            center_date = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
            date_from = (center_date - timedelta(days=15)).strftime("%Y-%m-%d")
            date_to = (center_date + timedelta(days=15)).strftime("%Y-%m-%d")
            print(f"Searching files from {date_from} to {date_to}")
        else:
            date_from = '2020-01-01'
            date_to = None
        
        self.firds = FIRDSClient(date_from=date_from, date_to=date_to)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"Output directory: {self.output_dir.absolute()}")
    
    def list_available_files(self, file_type: str = None, asset_type: str = None, date: str = None):
        """
        List available FIRDS files matching criteria.
        
        Args:
            file_type: 'FULINS' or 'DLTINS'
            asset_type: Asset type letter (C, D, E, F, H, I, J, O, R, S)
            date: Date in YYYYMMDD format
        
        Returns:
            DataFrame with matching files
        """
        print("\n" + "="*60)
        print("Fetching FIRDS file list...")
        print("="*60)
        
        files = self.firds.get_file_list()
        print(f"Total files available: {len(files)}")
        
        # Filter by file type
        if file_type:
            if file_type.upper() == 'FULINS':
                pattern = r'^FULINS_'
            elif file_type.upper() == 'DLTINS':
                pattern = r'^DLTINS_'
            else:
                raise ValueError(f"Invalid file_type: {file_type}. Must be FULINS or DLTINS")
            
            files = files[files['file_name'].str.match(pattern, na=False)]
            print(f"Files matching type '{file_type}': {len(files)}")
        
        # Filter by asset type (only for FULINS)
        if asset_type and file_type and file_type.upper() == 'FULINS':
            pattern = rf'FULINS_{asset_type.upper()}_'
            files = files[files['file_name'].str.contains(pattern, na=False)]
            print(f"Files matching asset type '{asset_type}': {len(files)}")
        
        # Filter by date
        if date:
            pattern = rf'_{date}_'
            files = files[files['file_name'].str.contains(pattern, na=False)]
            print(f"Files matching date '{date}': {len(files)}")
        
        if len(files) == 0:
            print("\n⚠️  No files found matching criteria!")
            return pd.DataFrame()
        
        print("\nMatching files:")
        print(files[['file_name', 'file_type', 'publication_date']].to_string())
        
        return files
    
    def inspect_file(self, file_type: str, asset_type: str = None, date: str = None, 
                     file_index: int = 0, save_csv: bool = True):
        """
        Download and inspect a FIRDS file.
        
        Args:
            file_type: 'FULINS' or 'DLTINS'
            asset_type: Asset type letter (only for FULINS)
            date: Date in YYYYMMDD format (optional)
            file_index: Index of file to download if multiple matches (default: 0 = first)
            save_csv: Whether to save the data as CSV
        
        Returns:
            DataFrame with parsed data
        """
        # Get matching files
        files = self.list_available_files(file_type, asset_type, date)
        
        if files.empty:
            print("\n❌ No files to inspect!")
            return None
        
        # Select file
        if file_index >= len(files):
            print(f"\n⚠️  File index {file_index} out of range. Using index 0.")
            file_index = 0
        
        selected_file = files.iloc[file_index]
        file_name = selected_file['file_name']
        download_url = selected_file['download_link']
        
        print("\n" + "="*60)
        print(f"Inspecting file: {file_name}")
        print("="*60)
        print(f"URL: {download_url}")
        print(f"Publication date: {selected_file.get('publication_date', 'N/A')}")
        print(f"File type: {selected_file.get('file_type', 'N/A')}")
        
        # Download and parse
        print("\nDownloading and parsing... (this may take a minute)")
        try:
            df = self.firds.download_file(download_url, update=True)
        except Exception as e:
            print(f"\n❌ Error downloading/parsing file: {e}")
            return None
        
        # Display structure
        print("\n" + "="*60)
        print("FILE STRUCTURE")
        print("="*60)
        print(f"\nTotal records: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        
        print("\n--- All Columns ---")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:3d}. {col}")
        
        print("\n--- Data Types ---")
        print(df.dtypes.value_counts())
        
        print("\n--- Sample Data (First 10 rows) ---")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        print(df.head(10).to_string())
        
        # Non-null counts for key columns
        print("\n--- Non-Null Counts (First 20 Columns) ---")
        null_counts = df.count()
        print(null_counts.head(20).to_string())
        
        # Save to CSV if requested
        if save_csv:
            csv_filename = f"{file_name.replace('.zip', '')}_inspection.csv"
            csv_path = self.output_dir / csv_filename
            df.to_csv(csv_path, index=False)
            print(f"\n💾 Data saved to: {csv_path.absolute()}")
            
            # Also save column list
            col_filename = f"{file_name.replace('.zip', '')}_columns.txt"
            col_path = self.output_dir / col_filename
            with open(col_path, 'w') as f:
                f.write(f"File: {file_name}\n")
                f.write(f"Total Columns: {len(df.columns)}\n")
                f.write(f"Total Records: {len(df)}\n")
                f.write(f"\nColumn List:\n")
                f.write("="*60 + "\n")
                for i, col in enumerate(df.columns, 1):
                    f.write(f"{i:3d}. {col}\n")
            print(f"📋 Column list saved to: {col_path.absolute()}")
        
        return df
    
    def inspect_latest_by_asset_type(self, asset_type: str, save_csv: bool = True):
        """
        Inspect the latest FULINS file for a specific asset type.
        
        Args:
            asset_type: Asset type letter (C, D, E, F, H, I, J, O, R, S)
            save_csv: Whether to save the data as CSV
        
        Returns:
            DataFrame with parsed data
        """
        print(f"\n🔍 Looking for latest FULINS file for asset type: {asset_type}")
        
        files = self.list_available_files(file_type='FULINS', asset_type=asset_type)
        
        if files.empty:
            return None
        
        # Extract date and find latest
        files['date_extracted'] = files['file_name'].str.extract(r'_(\d{8})_')[0]
        latest_date = files['date_extracted'].max()
        
        print(f"\nLatest date found: {latest_date}")
        
        return self.inspect_file(
            file_type='FULINS',
            asset_type=asset_type,
            date=latest_date,
            file_index=0,
            save_csv=save_csv
        )
    
    def compare_asset_types(self, asset_types: list = None):
        """
        Compare column structures across different asset types.
        
        Args:
            asset_types: List of asset types to compare (default: all)
        """
        if asset_types is None:
            asset_types = ['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S']
        
        print("\n" + "="*60)
        print("COMPARING ASSET TYPES")
        print("="*60)
        
        all_columns = {}
        
        for asset_type in asset_types:
            print(f"\nProcessing asset type: {asset_type}")
            files = self.list_available_files(file_type='FULINS', asset_type=asset_type)
            
            if not files.empty:
                files['date_extracted'] = files['file_name'].str.extract(r'_(\d{8})_')[0]
                latest_date = files['date_extracted'].max()
                latest_file = files[files['date_extracted'] == latest_date].iloc[0]
                
                try:
                    df = self.firds.download_file(latest_file['download_link'], update=False)
                    all_columns[asset_type] = set(df.columns)
                    print(f"  ✓ {asset_type}: {len(df.columns)} columns, {len(df)} records")
                except Exception as e:
                    print(f"  ✗ {asset_type}: Error - {e}")
            else:
                print(f"  ⚠️  {asset_type}: No files found")
        
        # Find common and unique columns
        if len(all_columns) > 0:
            print("\n" + "="*60)
            print("COLUMN ANALYSIS")
            print("="*60)
            
            all_cols = set()
            for cols in all_columns.values():
                all_cols.update(cols)
            
            print(f"\nTotal unique columns across all types: {len(all_cols)}")
            
            # Common columns
            common = all_columns[list(all_columns.keys())[0]]
            for cols in all_columns.values():
                common = common.intersection(cols)
            
            print(f"Common columns (all asset types): {len(common)}")
            if common:
                print("  " + ", ".join(sorted(common)[:10]) + "...")
            
            # Asset-type specific columns
            print("\nAsset-type specific columns:")
            for asset_type, cols in all_columns.items():
                unique = cols - common
                if unique:
                    print(f"  {asset_type}: {len(unique)} unique columns")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Inspect FIRDS files to understand their structure',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Inspect latest FULINS file for equities
  python tools/inspect_firds_files.py --file-type FULINS --asset-type E
  
  # Inspect specific date
  python tools/inspect_firds_files.py --file-type FULINS --asset-type D --date 20241220
  
  # Inspect DLTINS file
  python tools/inspect_firds_files.py --file-type DLTINS --date 20241220
  
  # Compare all asset types
  python tools/inspect_firds_files.py --compare-asset-types
  
  # List available files only
  python tools/inspect_firds_files.py --file-type FULINS --list-only
        """
    )
    
    parser.add_argument('--file-type', type=str, choices=['FULINS', 'DLTINS'],
                        help='Type of FIRDS file to inspect')
    parser.add_argument('--asset-type', type=str,
                        help='Asset type letter (C, D, E, F, H, I, J, O, R, S)')
    parser.add_argument('--date', type=str,
                        help='Date in YYYYMMDD format')
    parser.add_argument('--file-index', type=int, default=0,
                        help='Index of file to inspect if multiple matches (default: 0)')
    parser.add_argument('--output-dir', type=str, default='downloads/inspections',
                        help='Output directory for CSV files')
    parser.add_argument('--no-save', action='store_true',
                        help='Do not save CSV files')
    parser.add_argument('--list-only', action='store_true',
                        help='Only list files, do not download')
    parser.add_argument('--compare-asset-types', action='store_true',
                        help='Compare column structures across asset types')
    parser.add_argument('--latest', action='store_true',
                        help='Automatically select latest file for asset type')
    
    args = parser.parse_args()
    
    # Create inspector (pass date to optimize file list query)
    inspector = FIRDSFileInspector(output_dir=args.output_dir, date=args.date)
    
    # Handle compare mode
    if args.compare_asset_types:
        inspector.compare_asset_types()
        return
    
    # Validate arguments
    if not args.file_type and not args.compare_asset_types:
        parser.print_help()
        print("\n❌ Error: --file-type is required (or use --compare-asset-types)")
        sys.exit(1)
    
    # Handle list-only mode
    if args.list_only:
        inspector.list_available_files(args.file_type, args.asset_type, args.date)
        return
    
    # Handle latest mode
    if args.latest and args.asset_type:
        inspector.inspect_latest_by_asset_type(args.asset_type, save_csv=not args.no_save)
        return
    
    # Regular inspection
    inspector.inspect_file(
        file_type=args.file_type,
        asset_type=args.asset_type,
        date=args.date,
        file_index=args.file_index,
        save_csv=not args.no_save
    )


if __name__ == '__main__':
    main()
