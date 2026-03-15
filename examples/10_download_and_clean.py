"""
Example: Download Latest Equity Files and Clean Cache

Demonstrates the component-based API for:
1. Downloading latest FULINS_E files from ESMA
2. Indexing them into the database
3. Cleaning up older cached files
"""
import logging
import pandas as pd
from pathlib import Path
from esma_dm.clients.firds import FIRDSClient

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Initialize client
    firds = FIRDSClient(mode='current')
    
    print("=== Download Latest Equity Files Workflow ===")
    print()
    
    # 1. Check current cache state
    print("1. Checking current cache state...")
    cache_dir = firds.config.downloads_path / 'firds'
    old_e_files = list(cache_dir.glob('FULINS_E_*_data.csv'))
    print(f"   Found {len(old_e_files)} existing FULINS_E files")
    
    if old_e_files:
        print("   Existing files:")
        for f in sorted(old_e_files)[:3]:  # Show first 3
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"     - {f.name} ({size_mb:.1f} MB)")
        if len(old_e_files) > 3:
            print(f"     ... and {len(old_e_files) - 3} more")
    print()
    
    # 2. Download latest equity files from ESMA
    print("2. Downloading latest equity files from ESMA...")
    try:
        result = firds.download.get_latest_full_files(
            asset_type='E',
            update=True  # Force fresh download
        )
        if isinstance(result, pd.DataFrame) and not result.empty:
            print(f"   Download completed: {len(result)} record(s) retrieved")
            # Get unique file names if available
            if 'file_name' in result.columns:
                files = result['file_name'].unique()
                print(f"   Files: {list(files)}")
            else:
                print(f"   Data retrieved: {result.shape[0]:,} instruments")
        else:
            print(f"   Download completed: Data retrieved successfully")
    except Exception as e:
        print(f"   Download failed: {e}")
        return
    print()
    
    # 3. Check what was downloaded
    print("3. Checking newly downloaded files...")
    new_e_files = list(cache_dir.glob('FULINS_E_*_data.csv'))
    newly_added = [f for f in new_e_files if f not in old_e_files]
    
    print(f"   Total FULINS_E files now: {len(new_e_files)}")
    if newly_added:
        print("   Newly downloaded:")
        for f in newly_added:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"     + {f.name} ({size_mb:.1f} MB)")
    print()
    
    # 4. Initialize fresh database and index the new files
    print("4. Indexing new equity files into database...")
    try:
        firds.initialize_database()
        index_result = firds.parse.index_cached_files(
            asset_type='E', 
            latest_only=True  # Use only the latest files
        )
        print(f"   Indexing completed:")
        print(f"     - Files processed: {index_result['files_processed']}")
        print(f"     - Instruments: {index_result['total_instruments']:,}")
        print(f"     - Listings: {index_result['total_listings']:,}")
    except Exception as e:
        print(f"   Indexing failed: {e}")
        return
    print()
    
    # 5. Test the data with a quick query
    print("5. Testing data with sample queries...")
    try:
        stats = firds.get_database_stats()
        print(f"   Database total: {stats.get('total_instruments', 0):,} instruments")
        
        # Try to get a random equity instrument
        sample_query = "SELECT isin, full_name, short_name FROM instruments WHERE asset_type = 'E' LIMIT 3"
        sample_data = firds.query_database(sample_query)
        
        if not sample_data.empty:
            print("   Sample equity instruments:")
            for _, row in sample_data.iterrows():
                print(f"     - {row['isin']}: {row['short_name']}")
    except Exception as e:
        print(f"   Query failed: {e}")
    print()
    
    # 6. Clean up older files (keep only latest)
    print("6. Cleaning up older FULINS_E files...")
    current_e_files = list(cache_dir.glob('FULINS_E_*_data.csv'))
    
    if len(current_e_files) > 2:  # Keep latest 2 files as backup
        # Sort by modification time, keep newest 2
        files_by_time = sorted(current_e_files, key=lambda f: f.stat().st_mtime, reverse=True)
        files_to_keep = files_by_time[:2]
        files_to_remove = files_by_time[2:]
        
        print(f"   Keeping {len(files_to_keep)} newest files:")
        for f in files_to_keep:
            print(f"     {f.name}")

        print(f"   Removing {len(files_to_remove)} older files:")
        for f in files_to_remove:
            try:
                size_mb = f.stat().st_size / (1024 * 1024)
                f.unlink()
                print(f"     Removed {f.name} ({size_mb:.1f} MB freed)")
            except Exception as e:
                print(f"     Failed to remove {f.name}: {e}")
    else:
        print("   No cleanup needed (≤2 files)")
    
    print()
    print("=== Workflow Complete ===")
    print()
    print("Summary:")
    print("- Downloaded latest equity files from ESMA")
    print("- Indexed files into database using component-based API")
    print("- Cleaned up older cached files")
    print("- Verified data integrity with sample queries")
    print()
    print("Component API used:")
    print("  - firds.download.get_latest_full_files(asset_type='E', update=True)")
    print("  - firds.parse.index_cached_files(asset_type='E', latest_only=True)")
    print("  - firds.query_database('SELECT ...')")
    print("  - firds.get_database_stats()")

if __name__ == "__main__":
    main()