"""
Command-line entry point for ESMA Data Manager.
"""

from esma_dm.cli import cli


def main():
    """Main CLI entry point."""
    cli()


if __name__ == '__main__':
    main()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)