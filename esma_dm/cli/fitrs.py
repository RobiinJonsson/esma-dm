"""
FITRS file management commands for ESMA Data Manager CLI.
"""

import click
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

from esma_dm.file_manager import FITRSFileManager, FITRSFileType as FileType, InstrumentType
from esma_dm.config import Config

console = Console()


@click.group(name='fitrs')
def fitrs_cli():
    """FITRS (Transparency) file management commands."""
    pass


@fitrs_cli.command(name='list')
@click.option('--type', 'file_type',
              type=click.Choice(['FULECR', 'DLTECR', 'FULNCR', 'DLTNCR'], case_sensitive=False),
              help='Filter by file type')
@click.option('--instrument', 'instrument_type',
              type=click.Choice(['equity', 'non-equity'], case_sensitive=False),
              help='Filter by instrument type')
@click.option('--date-from',
              help='Start date (YYYY-MM-DD)')
@click.option('--date-to',
              help='End date (YYYY-MM-DD)')
@click.option('--limit', default=None, type=int,
              help='Maximum number of files to list (omit for all files)')
@click.option('--fetch-all', is_flag=True, default=False,
              help='Fetch all available files using pagination')
def list_files(file_type: Optional[str], instrument_type: Optional[str],
               date_from: Optional[str], date_to: Optional[str],
               limit: Optional[int], fetch_all: bool):
    """
    List available FITRS transparency files from ESMA register.
    
    By default, fetches all available files using pagination.
    
    Examples:
    
        esma-dm fitrs list --type FULECR
        
        esma-dm fitrs list --instrument equity
        
        esma-dm fitrs list --date-from 2026-01-01 --fetch-all
    """
    try:
        console.print("\n[bold cyan]Fetching FITRS file list from ESMA...[/bold cyan]\n")
        
        # Initialize file manager
        config = Config()
        date_to_use = date_to or datetime.today().strftime('%Y-%m-%d')
        date_from_use = date_from or '2018-01-01'
        
        cache_dir = config.downloads_path / 'fitrs'
        manager = FITRSFileManager(
            cache_dir=cache_dir,
            date_from=date_from_use,
            date_to=date_to_use
        )
        
       # Get file list
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Loading...", total=None)
            
            # Enable fetch_all by default unless limit is specified
            should_fetch_all = fetch_all or (limit is None)
            
            files = manager.list_files(
                file_type=file_type,
                instrument_type=instrument_type,
                date_from=date_from,
                date_to=date_to,
                fetch_all=should_fetch_all
            )
        
        if not files:
            console.print("[yellow]No files found matching the criteria.[/yellow]\n")
            return
        
        # Apply limit if specified
        if limit:
            files = files[:limit]
        
        # Create rich table
        table = Table(title="ESMA FITRS Files", box=box.ROUNDED, show_lines=False)
        table.add_column("File Name", style="cyan", no_wrap=False, max_width=40)
        table.add_column("Type", style="magenta", width=10)
        table.add_column("Instrument", style="blue", width=12)
        table.add_column("Date", style="green", width=12)
        table.add_column("Parts", style="yellow", width=10)
        
        for file_obj in files:
            table.add_row(
                file_obj.filename,
                file_obj.file_type,
                file_obj.instrument_type or "N/A",
                file_obj.publication_date.strftime('%Y-%m-%d'),
                f"{file_obj.part_number or 1}/{file_obj.total_parts or 1}"
            )
        
        console.print(table)
        console.print(f"\n[bold green]Total files: {len(files)}[/bold green]\n")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@fitrs_cli.command(name='download')
@click.option('--instrument', 'instrument_type',
              type=click.Choice(['equity', 'non-equity'], case_sensitive=False),
              required=True,
              help='Instrument type to download')
@click.option('--asset', 'asset_type',
              type=click.Choice(['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S'],
                                case_sensitive=False),
              default=None,
              help='CFI asset type filter (e.g. E=Equities, D=Debt, S=Swaps). '
                   'Omit to download all asset types for the instrument.')
@click.option('--update', is_flag=True, default=False,
              help='Force re-download (ignore cache)')
def download_files(instrument_type: str, asset_type: Optional[str], update: bool):
    """
    Download latest FITRS full files for an instrument type.

    Optionally filter to a single CFI asset type with --asset.

    Examples:

        esma-dm fitrs download --instrument equity

        esma-dm fitrs download --instrument equity --asset E

        esma-dm fitrs download --instrument non-equity --asset D

        esma-dm fitrs download --instrument non-equity --update
    """
    label = instrument_type
    if asset_type:
        label = f"{instrument_type} / asset {asset_type.upper()}"
    try:
        console.print(f"\n[bold cyan]Downloading {label} FITRS files...[/bold cyan]\n")

        # Initialize file manager
        config = Config()
        cache_dir = config.downloads_path / 'fitrs'
        manager = FITRSFileManager(cache_dir=cache_dir)

        # Download files
        paths = manager.download_latest_full_files(
            instrument_type=instrument_type,
            asset_type=asset_type,
            update=update
        )

        if not paths:
            console.print("[yellow]No files downloaded.[/yellow]\n")
            return
        
        # Display results
        console.print(f"\n[bold green]Downloaded {len(paths)} file(s):[/bold green]")
        for p in paths:
            size_mb = p.stat().st_size / (1024 * 1024)
            console.print(f"  • {p.name} ({size_mb:.2f} MB)")
        
        console.print()
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@fitrs_cli.command(name='cache')
@click.option('--type', 'file_type',
              type=click.Choice(['FULECR', 'DLTECR', 'FULNCR', 'DLTNCR'], case_sensitive=False),
              help='Filter by file type')
@click.option('--instrument', 'instrument_type',
              type=click.Choice(['equity', 'non-equity'], case_sensitive=False),
              help='Filter by instrument type')
def list_cache(file_type: Optional[str], instrument_type: Optional[str]):
    """
    List cached FITRS files.
    
    Examples:
    
        esma-dm fitrs cache
        
        esma-dm fitrs cache --instrument equity
        
        esma-dm fitrs cache --type FULECR
    """
    try:
        # Initialize file manager
        config = Config()
        cache_dir = config.downloads_path / 'fitrs'
        manager = FITRSFileManager(cache_dir=cache_dir)
        
        # Get cached files
        files = manager.get_cached_files(
            file_type=file_type,
            instrument_type=instrument_type
        )
        
        if not files:
            console.print("\n[yellow]No cached files found.[/yellow]\n")
            return
        
        # Create table
        table = Table(title="Cached FITRS Files", box=box.ROUNDED)
        table.add_column("File Name", style="cyan", no_wrap=False)
        table.add_column("Size (MB)", style="magenta", justify="right")
        table.add_column("Modified", style="green")
        
        total_size = 0
        for f in files:
            size_mb = f.stat().st_size / (1024 * 1024)
            total_size += size_mb
            mod_time = datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
            
            table.add_row(
                f.name,
                f"{size_mb:.2f}",
                mod_time
            )
        
        console.print(table)
        console.print(f"\n[bold]Total: {len(files)} file(s), {total_size:.2f} MB[/bold]\n")
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@fitrs_cli.command(name='stats')
def stats():
    """
    Show FITRS cache statistics.
    
    Displays file counts by type and instrument type.
    
    Example:
    
        esma-dm fitrs stats
    """
    try:
        # Initialize file manager
        config = Config()
        cache_dir = config.downloads_path / 'fitrs'
        manager = FITRSFileManager(cache_dir=cache_dir)
        
        # Get stats
        stats_data = manager.get_file_stats()
        
        console.print("\n[bold cyan]FITRS Cache Statistics[/bold cyan]\n")
        
        # Overall stats
        console.print(f"Total files: [bold]{stats_data['total_files']}[/bold]")
        console.print(f"Total size: [bold]{stats_data['total_size_mb']:.2f} MB[/bold]\n")
        
        # By file type
        if stats_data['by_file_type']:
            table1 = Table(title="Files by Type", box=box.SIMPLE)
            table1.add_column("File Type", style="cyan")
            table1.add_column("Count", justify="right", style="magenta")
            
            for ftype, count in sorted(stats_data['by_file_type'].items()):
                table1.add_row(ftype, str(count))
            
            console.print(table1)
            console.print()
        
        # By instrument type
        table2 = Table(title="Files by Instrument Type", box=box.SIMPLE)
        table2.add_column("Instrument Type", style="blue")
        table2.add_column("Count", justify="right", style="magenta")
        
        for itype, count in stats_data['by_instrument_type'].items():
            if count > 0:
                table2.add_row(itype.replace('_', '-'), str(count))
        
        console.print(table2)
        console.print()
        
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@fitrs_cli.command(name='types')
def show_types():
    """
    List available FITRS file types with descriptions.
    
    Example:
    
        esma-dm fitrs types
    """
    console.print("\n[bold cyan]FITRS File Types[/bold cyan]\n")
    
    # File types table
    table1 = Table(title="File Types", box=box.ROUNDED)
    table1.add_column("Code", style="cyan", width=12)
    table1.add_column("Description", style="white")
    
    descriptions = {
        'FULECR': 'Full Equity Comprehensive Report (full snapshot)',
        'DLTECR': 'Delta Equity Comprehensive Report (updates)',
        'FULNCR': 'Full Non-Equity Comprehensive Report (full snapshot)',
        'DLTNCR': 'Delta Non-Equity Comprehensive Report (updates)',
        'FULNCR_NYAR': 'Non-Equity Subclass Yearly',
        'FULNCR_SISC': 'Non-Equity Subclass SI'
    }
    
    for code, desc in descriptions.items():
        table1.add_row(code, desc)
    
    console.print(table1)
    
    # Instrument types
    console.print("\n[bold cyan]Instrument Types[/bold cyan]\n")
    table2 = Table(box=box.ROUNDED)
    table2.add_column("Type", style="blue", width=15)
    table2.add_column("Description", style="white")
    
    table2.add_row("equity", "Equity instruments (stocks, shares)")
    table2.add_row("non-equity", "Non-equity instruments (bonds, derivatives, etc.)")
    
    console.print(table2)
    console.print()


@fitrs_cli.command(name='fields')
@click.argument('file_path', type=click.Path())
def list_fields(file_path: str):
    """
    List all field names (columns) in a CSV file.
    
    FILE_PATH: Path to the CSV file (can be in cache or absolute path)
    
    Examples:
    
        esma-dm fitrs fields downloads/data/fitrs/FULECR_20260207_01of01.csv
    """
    try:
        file_path_obj = Path(file_path)
        
        # If relative path, try cache directory
        if not file_path_obj.is_absolute():
            config = Config()
            cache_path = config.downloads_path / 'fitrs' / file_path
            if cache_path.exists():
                file_path_obj = cache_path
        
        if not file_path_obj.exists():
            console.print(f"\n[bold red]Error:[/bold red] File not found: {file_path}\n")
            raise click.Abort()
        
        console.print(f"\n[bold cyan]Reading file:[/bold cyan] {file_path_obj.name}\n")
        
        # Read just the header
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Loading...", total=None)
            df = pd.read_csv(file_path_obj, nrows=0)
        
        # Create rich table
        table = Table(title="File Fields", box=box.ROUNDED, show_lines=False)
        table.add_column("#", style="dim", width=4)
        table.add_column("Field Name", style="cyan")
        table.add_column("Data Type", style="green")
        
        for idx, (col, dtype) in enumerate(df.dtypes.items(), 1):
            table.add_row(str(idx), col, str(dtype))
        
        console.print(table)
        console.print(f"\n[dim]Total fields: {len(df.columns)}[/dim]\n")
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@fitrs_cli.command(name='head')
@click.argument('file_path', type=click.Path())
@click.option('--rows', '-n', default=10, show_default=True,
              help='Number of rows to display')
@click.option('--columns', '-c',
              help='Comma-separated list of columns to display')
def show_head(file_path: str, rows: int, columns: Optional[str]):
    """
    Display the first N rows of a CSV file.
    
    FILE_PATH: Path to the CSV file (can be in cache or absolute path)
    
    Examples:
    
        esma-dm fitrs head downloads/data/fitrs/FULECR_20260207_01of01.csv
        
        esma-dm fitrs head FULECR_20260207_01of01.csv --rows 20
        
        esma-dm fitrs head FULECR_20260207_01of01.csv --columns "ISIN,FullNm,TradgVn"
    """
    try:
        file_path_obj = Path(file_path)
        
        # If relative path, try cache directory
        if not file_path_obj.is_absolute():
            config = Config()
            cache_path = config.downloads_path / 'fitrs' / file_path
            if cache_path.exists():
                file_path_obj = cache_path
        
        if not file_path_obj.exists():
            console.print(f"\n[bold red]Error:[/bold red] File not found: {file_path}\n")
            raise click.Abort()
        
        console.print(f"\n[bold cyan]Reading file:[/bold cyan] {file_path_obj.name}\n")
        
        # Read file
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Loading...", total=None)
            df = pd.read_csv(file_path_obj, nrows=rows)
        
        # Filter columns if specified
        if columns:
            col_list = [c.strip() for c in columns.split(',')]
            # Check if columns exist
            missing = [c for c in col_list if c not in df.columns]
            if missing:
                console.print(f"[bold red]Error:[/bold red] Columns not found: {', '.join(missing)}\n")
                console.print(f"[dim]Available columns: {', '.join(df.columns.tolist())}[/dim]\n")
                raise click.Abort()
            df = df[col_list]
        
        # Create rich table
        table = Table(
            title=f"First {len(df)} rows of {file_path_obj.name}",
            box=box.ROUNDED,
            show_lines=True
        )
        
        # Add columns
        for col in df.columns:
            table.add_column(col, style="cyan", overflow="fold", max_width=40)
        
        # Add rows
        for _, row in df.iterrows():
            table.add_row(*[str(val) for val in row])
        
        console.print(table)
        console.print(f"\n[dim]Showing {len(df)} of {rows} requested rows[/dim]\n")
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@fitrs_cli.command(name='index')
@click.option('--type', 'file_types', multiple=True,
              type=click.Choice(['FULECR', 'DLTECR', 'FULNCR', 'DLTNCR'], case_sensitive=False),
              help='File type(s) to load. Repeat to specify multiple (default: all cached types).')
@click.option('--mode', default='current', type=click.Choice(['current', 'history'], case_sensitive=False),
              help='Database mode (default: current)')
def index_from_cache(file_types, mode: str):
    """
    Load FITRS transparency data from locally cached CSV files.

    Reads all *_data.csv files in the FITRS cache and inserts them into
    the transparency tables without re-downloading from ESMA.

    Examples:

        esma-dm fitrs index

        esma-dm fitrs index --type FULECR --type FULNCR

        esma-dm fitrs index --type FULECR
    """
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

    try:
        from esma_dm.clients.fitrs import FITRSClient

        types_filter = list(file_types) if file_types else None
        label = ', '.join(types_filter) if types_filter else 'all types'
        console.print(f"\n[bold cyan]Indexing FITRS cache[/bold cyan]  ({label})\n")

        fitrs = FITRSClient(mode=mode)

        # Pre-scan to know the file count for the progress bar
        import re
        from esma_dm.config import Config
        cache_dir = Config(mode=mode).downloads_path / 'fitrs'
        pattern = re.compile(r'^(FULECR|DLTECR|FULNCR|DLTNCR)_(\d{8})_')
        eligible_files = [
            p for p in sorted(cache_dir.glob('*_data.csv'))
            if pattern.match(p.name) and (not types_filter or pattern.match(p.name).group(1) in types_filter)
        ]

        if not eligible_files:
            console.print("[yellow]No matching cached files found.[/yellow]\n")
            return

        console.print(f"[dim]Found {len(eligible_files)} file(s) to process.[/dim]\n")

        results = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}", justify="left"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Indexing...", total=len(eligible_files))

            def on_progress(filename, current, total):
                progress.update(task, completed=current - 1, description=filename)

            result = fitrs.index_cached_files(
                file_types=types_filter,
                progress_callback=on_progress,
            )
            progress.update(task, completed=len(eligible_files), description="Done")

        console.print()

        # Summary table
        summary = Table(title="Indexing Summary", box=box.ROUNDED)
        summary.add_column("File", style="cyan")
        summary.add_column("Type", style="magenta", width=8)
        summary.add_column("Records", style="white", justify="right")
        summary.add_column("Status", style="green")

        for d in result['details']:
            status_str = d['status']
            status_style = "green" if status_str == "ok" else ("yellow" if status_str == "empty" else "red")
            summary.add_row(
                d['file'],
                d.get('file_type', '—'),
                f"{d['records']:,}",
                f"[{status_style}]{status_str}[/{status_style}]",
            )

        console.print(summary)
        console.print()
        console.print(f"[bold green]Total records indexed:[/bold green] {result['total_records']:,}")
        console.print(f"[dim]Files processed: {result['files_processed']}  |  Skipped: {result['files_skipped']}[/dim]\n")

    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


# Export commands
__all__ = ['fitrs_cli']
