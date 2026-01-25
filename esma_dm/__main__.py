"""
Command-line entry point for ESMA Data Manager.
Handles initialization, status checks, and other CLI operations.
"""
import sys


def show_help():
    """Show help message."""
    print("🏦 ESMA Data Manager - Command Line Interface")
    print("=" * 50)
    print("\nUsage:")
    print("  python -m esma_dm                 - Interactive initialization")
    print("  python -m esma_dm init            - Interactive initialization")
    print("  python -m esma_dm status          - Show database status")
    print("  python -m esma_dm status current  - Show current mode status")
    print("  python -m esma_dm status history  - Show history mode status")
    print("  python -m esma_dm help            - Show this help")
    print("\nConsole Commands (after installation):")
    print("  esma-dm-init    - Interactive initialization")
    print("  esma-dm status  - Show database status")
    print("\nFor more information, see the documentation.")


def main():
    """Main CLI dispatcher."""
    if len(sys.argv) == 1:
        # No arguments - run initialization
        from esma_dm.initialize import main as init_main
        init_main()
        return
    
    command = sys.argv[1].lower()
    
    if command in ['init', 'initialize', 'setup']:
        from esma_dm.initialize import main as init_main
        init_main()
        
    elif command in ['status', 'info', 'stats']:
        from esma_dm.status import main as status_main
        status_main()
        
    elif command in ['help', '--help', '-h']:
        show_help()
        
    else:
        print(f"❌ Unknown command: {command}")
        print("Run 'python -m esma_dm help' for available commands.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)