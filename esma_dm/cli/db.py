"""
Database management commands for ESMA Data Manager CLI.
"""

import os
import click
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

ASSET_LABELS = {
    'C': 'Collective Investment Vehicles',
    'D': 'Debt Instruments',
    'E': 'Equities',
    'F': 'Futures',
    'H': 'Non-Standard Derivatives',
    'I': 'Spot Instruments',
    'J': 'Forwards',
    'K': 'Strategies',
    'L': 'Financing',
    'M': 'Other',
    'O': 'Listed Options',
    'R': 'Rights & Entitlements',
    'S': 'Swaps',
    'T': 'Referential',
}

DETAIL_TABLES = {
    'E': 'equity_instruments',
    'D': 'debt_instruments',
    'F': 'futures_instruments',
    'O': 'option_instruments',
    'S': 'swap_instruments',
    'J': 'forward_instruments',
    'R': 'rights_instruments',
    'C': 'civ_instruments',
    'I': 'spot_instruments',
    'H': 'non_standard_instruments',
    'K': 'strategy_instruments',
    'L': 'financing_instruments',
    'M': 'other_instruments',
    'T': 'referential_instruments',
}


@click.group(name='db')
def db_cli():
    """Database management commands (stats, re-init, drop)."""
    pass


@db_cli.command(name='stats')
@click.option('--mode', default='current', type=click.Choice(['current', 'history'], case_sensitive=False),
              help='Database mode (default: current)')
@click.option('--tables', is_flag=True, default=False,
              help='Include per-table row counts')
def show_stats(mode: str, tables: bool):
    """
    Show database statistics.

    Displays total instruments, listings breakdown by asset type,
    database file size, and optional per-table row counts.

    Examples:

        esma-dm db stats

        esma-dm db stats --tables
    """
    try:
        from esma_dm.clients.firds import FIRDSClient

        firds = FIRDSClient(mode=mode)
        store = firds.data_store
        con = store.con

        console.print()

        # --- DB file info ---
        db_path = Path(store.db_path)
        db_size_mb = db_path.stat().st_size / 1_048_576 if db_path.exists() else 0

        info_table = Table(title="Database", box=box.ROUNDED)
        info_table.add_column("Property", style="cyan", width=20)
        info_table.add_column("Value", style="white")
        info_table.add_row("Mode", mode)
        info_table.add_row("Path", str(db_path))
        info_table.add_row("Size", f"{db_size_mb:,.1f} MB")
        console.print(info_table)
        console.print()

        # --- Instrument totals ---
        total = con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
        listings = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        distinct_venues = con.execute("SELECT COUNT(DISTINCT trading_venue_id) FROM listings").fetchone()[0]

        summary_table = Table(title="Summary", box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan", width=28)
        summary_table.add_column("Count", style="white", justify="right")
        summary_table.add_row("Total instruments", f"{total:,}")
        summary_table.add_row("Total listings", f"{listings:,}")
        summary_table.add_row("Distinct trading venues", f"{distinct_venues:,}")
        console.print(summary_table)
        console.print()

        # --- By asset type ---
        by_type = con.execute(
            "SELECT instrument_type, COUNT(*) FROM instruments GROUP BY instrument_type ORDER BY instrument_type"
        ).fetchall()

        if by_type:
            type_table = Table(title="Instruments by Asset Type", box=box.ROUNDED)
            type_table.add_column("Code", style="magenta", width=6)
            type_table.add_column("Description", style="white")
            type_table.add_column("Count", style="cyan", justify="right")
            type_table.add_column("Detail Table", style="dim")

            for code, count in by_type:
                label = ASSET_LABELS.get(code, code or '—')
                detail = DETAIL_TABLES.get(code, '—')
                type_table.add_row(code or '—', label, f"{count:,}", detail)

            console.print(type_table)
            console.print()

        # --- FITRS tables ---
        fitrs_tables = ['transparency', 'subclass_transparency', 'equity_transparency',
                        'non_equity_transparency', 'transparency_metadata']
        fitrs_rows = []
        for t in fitrs_tables:
            try:
                n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                fitrs_rows.append((t, n))
            except Exception:
                fitrs_rows.append((t, None))

        if any(r[1] for r in fitrs_rows):
            fitrs_table = Table(title="FITRS Transparency Tables", box=box.ROUNDED)
            fitrs_table.add_column("Table", style="cyan")
            fitrs_table.add_column("Rows", style="white", justify="right")
            for name, count in fitrs_rows:
                fitrs_table.add_row(name, f"{count:,}" if count is not None else "[dim]—[/dim]")
            console.print(fitrs_table)
            console.print()

        # --- Optional: all tables ---
        if tables:
            all_tables = con.execute("SHOW TABLES").fetchall()
            all_table = Table(title="All Tables", box=box.SIMPLE)
            all_table.add_column("Table", style="cyan")
            all_table.add_column("Rows", style="white", justify="right")
            for (tname,) in sorted(all_tables):
                try:
                    n = con.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
                    all_table.add_row(tname, f"{n:,}")
                except Exception:
                    all_table.add_row(tname, "[dim]error[/dim]")
            console.print(all_table)
            console.print()

    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@db_cli.command(name='reinit')
@click.option('--mode', default='current', type=click.Choice(['current', 'history'], case_sensitive=False),
              help='Database mode (default: current)')
@click.option('--yes', is_flag=True, default=False,
              help='Skip confirmation prompt')
@click.option('--fitrs', 'load_fitrs', is_flag=True, default=False,
              help='Also load FITRS transparency data from cache after reinit')
def reinit_db(mode: str, yes: bool, load_fitrs: bool):
    """
    Drop and reinitialize the database schema.

    All data is permanently deleted. Use this to reset a corrupted or
    outdated database before re-indexing from cache.

    Examples:

        esma-dm db reinit

        esma-dm db reinit --mode history --yes

        esma-dm db reinit --fitrs
    """
    try:
        from esma_dm.clients.firds import FIRDSClient
        from esma_dm.config import Config

        config = Config(mode=mode)
        db_path = Path(config.get_database_path(mode))

        console.print()
        console.print(f"[bold yellow]Database:[/bold yellow] {db_path}")

        if db_path.exists():
            size_mb = db_path.stat().st_size / 1_048_576
            console.print(f"[bold yellow]Size:[/bold yellow] {size_mb:,.1f} MB")

            if not yes:
                click.confirm(
                    "\nThis will permanently delete all data. Continue?",
                    abort=True
                )
        else:
            console.print("[dim]Database file does not exist — initializing from scratch.[/dim]")

        firds = FIRDSClient(mode=mode)
        firds.data_store.connection.drop(confirm=True)
        console.print("[green]Dropped.[/green]")

        firds2 = FIRDSClient(mode=mode)
        firds2.data_store.connection.initialize(mode=mode)
        console.print("[green]Schema initialized.[/green]")

        new_size_mb = Path(firds2.data_store.db_path).stat().st_size / 1_048_576
        console.print(f"\n[bold cyan]Done.[/bold cyan] Fresh database at {firds2.data_store.db_path} ({new_size_mb:.1f} MB)")

        if load_fitrs:
            console.print("\n[bold cyan]Loading FITRS cache...[/bold cyan]\n")
            from esma_dm.clients.fitrs import FITRSClient
            from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
            import re

            cache_dir = config.downloads_path / 'fitrs'
            pattern = re.compile(r'^(FULECR|DLTECR|FULNCR|DLTNCR)_(\d{8})_')
            eligible_files = [p for p in sorted(cache_dir.glob('*_data.csv')) if pattern.match(p.name)]

            if not eligible_files:
                console.print("[yellow]No FITRS cached files found — skipping.[/yellow]")
            else:
                fitrs = FITRSClient(mode=mode)
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("Indexing FITRS...", total=len(eligible_files))

                    def on_prog(filename, current, total):
                        progress.update(task, completed=current - 1, description=filename)

                    result = fitrs.index_cached_files(progress_callback=on_prog)
                    progress.update(task, completed=len(eligible_files), description="Done")

                console.print(f"\n[green]FITRS:[/green] {result['total_records']:,} records from {result['files_processed']} files.")
        else:
            console.print(f"[dim]Run 'esma-dm fitrs index' to load transparency data from cache.[/dim]")

        console.print()

    except click.Abort:
        console.print("\n[yellow]Aborted.[/yellow]\n")
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@db_cli.command(name='drop')
@click.option('--mode', default='current', type=click.Choice(['current', 'history'], case_sensitive=False),
              help='Database mode (default: current)')
@click.option('--yes', is_flag=True, default=False,
              help='Skip confirmation prompt')
def drop_db(mode: str, yes: bool):
    """
    Drop the database file without reinitializing.

    Use 'reinit' instead if you want to immediately recreate the schema.

    Examples:

        esma-dm db drop --yes
    """
    try:
        from esma_dm.clients.firds import FIRDSClient
        from esma_dm.config import Config

        config = Config(mode=mode)
        db_path = Path(config.get_database_path(mode))

        console.print()

        if not db_path.exists():
            console.print(f"[yellow]Database not found:[/yellow] {db_path}\n")
            return

        size_mb = db_path.stat().st_size / 1_048_576
        console.print(f"[bold yellow]Database:[/bold yellow] {db_path}  ({size_mb:,.1f} MB)")

        if not yes:
            click.confirm("\nPermanently delete this database?", abort=True)

        firds = FIRDSClient(mode=mode)
        firds.data_store.connection.drop(confirm=True)
        console.print(f"\n[green]Dropped:[/green] {db_path}\n")

    except click.Abort:
        console.print("\n[yellow]Aborted.[/yellow]\n")
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()
