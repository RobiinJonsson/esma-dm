"""
Status checker for ESMA Data Manager.
Provides information about current database state and configuration.
"""
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from esma_dm import FIRDSClient
from esma_dm.utils.constants import ASSET_TYPE_CODES


def get_database_status(mode: str = 'current') -> Dict:
    """
    Get comprehensive status of the ESMA database.
    
    Args:
        mode: Database mode to check ('current' or 'history')
        
    Returns:
        Dictionary with status information
    """
    try:
        client = FIRDSClient(mode=mode)
        
        # Check if database exists
        db_path = client.data_store.db_path
        db_exists = Path(db_path).exists()
        
        status = {
            'mode': mode,
            'database_path': str(db_path),
            'database_exists': db_exists,
            'initialized': False,
            'last_updated': None,
            'size_mb': 0,
            'statistics': {}
        }
        
        if not db_exists:
            status['message'] = "Database not found. Run 'esma-dm-init' to initialize."
            return status
        
        # Get database size
        status['size_mb'] = round(Path(db_path).stat().st_size / (1024 * 1024), 2)
        
        # Try to connect and get stats
        try:
            result = client.data_store.initialize()
            status['initialized'] = True
            
            # Get statistics
            stats = client.get_store_stats()
            status['statistics'] = stats
            
            # Get last update time from metadata if available
            try:
                metadata = client.data_store.con.execute(
                    "SELECT MAX(indexed_at) as last_update FROM instruments"
                ).fetchone()
                if metadata and metadata[0]:
                    status['last_updated'] = metadata[0]
            except:
                pass
                
            # Success message
            total_instruments = stats.get('total_instruments', 0)
            if total_instruments > 0:
                status['message'] = f"✅ Database active with {total_instruments:,} instruments"
            else:
                status['message'] = "⚠️ Database initialized but empty"
                
        except Exception as e:
            status['message'] = f"❌ Database connection failed: {e}"
            
    except Exception as e:
        status = {
            'error': str(e),
            'message': f"❌ Error checking status: {e}"
        }
    
    return status


def print_status_report(mode: Optional[str] = None):
    """
    Print a formatted status report.
    
    Args:
        mode: Specific mode to check, or None for both
    """
    print("🔍 ESMA Data Manager - Status Report")
    print("=" * 50)
    print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    modes_to_check = [mode] if mode else ['current', 'history']
    
    for check_mode in modes_to_check:
        print(f"📊 {check_mode.upper()} MODE")
        print("-" * 20)
        
        status = get_database_status(check_mode)
        
        print(f"Database Path: {status.get('database_path', 'Unknown')}")
        print(f"Database Exists: {'✅ Yes' if status.get('database_exists') else '❌ No'}")
        print(f"Initialized: {'✅ Yes' if status.get('initialized') else '❌ No'}")
        
        if status.get('size_mb'):
            print(f"Size: {status['size_mb']} MB")
        
        if status.get('last_updated'):
            print(f"Last Updated: {status['last_updated']}")
            
        print(f"Status: {status.get('message', 'Unknown')}")
        
        # Show asset breakdown if available
        stats = status.get('statistics', {})
        asset_counts = stats.get('asset_type_counts', {})
        
        if asset_counts:
            print("\nAsset Type Breakdown:")
            for asset_type, count in asset_counts.items():
                name = ASSET_TYPE_CODES.get(asset_type, asset_type)
                print(f"  {asset_type} ({name}): {count:,}")
        
        if len(modes_to_check) > 1:
            print()
    
    print("\nℹ️  Available Commands:")
    print("  esma-dm-init    - Initialize/setup database")
    print("  esma-dm status  - Show this status report")
    print("  python -m esma_dm - Interactive initialization")


def main():
    """Main entry point for status command."""
    import sys
    
    mode = None
    if len(sys.argv) > 2 and sys.argv[2] in ['current', 'history']:
        mode = sys.argv[2]
    
    print_status_report(mode)


if __name__ == "__main__":
    main()