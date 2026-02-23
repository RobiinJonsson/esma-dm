"""
DVCAP CLI commands for file management.

Provides command-line interface for managing DVCAP (Double Volume Cap) files.
"""

import click
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
import pandas as pd

from esma_dm.file_manager.dvcap import DVCAPFileManager
from esma_dm.config import Config

console = Console()


@click.group(name='dvcap')
def dvcap_cli():
    """
    DVCAP (Double Volume Cap) file management commands.
    
    Manage DVCAP data files including listing, downloading, and caching operations.
    """
    pass


@dvcap_cli.command(name='list')
@click.option('--date-from', default=None, help='Start date (YYYY-MM-DD)')
@click.option('--date-to', default=None, help='End date (YYYY-MM-DD)')
@click.option('--limit', type=int, default=None, help='Maximum number of results')
def list_command(date_from, date_to, limit):
    """List available DVCAP files from ESMA register."""
    try:
        config = Config()
        manager = DVCAPFileManager(
            cache_dir=config.downloads_path / 'dvcap',
            date_from=date_from,
            date_to=date_to
        )
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching DVCAP files...", total=None)
            files = manager.list_files(fetch_all=True)
            progress.update(task, completed=True)
        
        if not files:
            console.print("[yellow]No files found matching criteria[/yellow]")
            return
        
        # Apply limit if specified
        if limit:
            files = files[:limit]
        
        # Create table
        table = Table(title=f"DVCAP Files ({len(files)} files)")
        table.add_column("Filename", style="cyan")
        table.add_column("Publication Date", style="green")
        table.add_column("Download Link", style="blue", max_width=50)
        
        for file in files:
            table.add_row(
                file.filename,
                file.publication_date.strftime('%Y-%m-%d'),
                file.download_link[:50] + '...' if len(file.download_link) > 50 else file.download_link
            )
        
        console.print(table)
        console.print(f"\n[green]✓ Found {len(files)} DVCAP files[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@dvcap_cli.command(name='download')
@click.option('--update', is_flag=True, help='Force fresh download (ignore cache)')
def download_command(update):
    """Download the latest DVCAP file."""
    try:
        config = Config()
        manager = DVCAPFileManager(cache_dir=config.downloads_path / 'dvcap')
        
        console.print("[cyan]Downloading latest DVCAP file...[/cyan]")
        
        file_path = manager.download_latest_file(update=update)
        
        if file_path:
            console.print(f"\n[green]✓ Downloaded: {file_path.name}[/green]")
            console.print(f"Location: {file_path}")
            
            # Show preview
            try:
                df = pd.read_csv(file_path, nrows=5)
                console.print(f"\n[cyan]Preview ({len(df)} rows shown):[/cyan]")
                console.print(df.to_string(index=False))
            except Exception as e:
                console.print(f"[yellow]Could not preview: {e}[/yellow]")
        else:
            console.print("[yellow]No file downloaded[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@dvcap_cli.command(name='cache')
@click.option('--clear', is_flag=True, help='Clear cache directory')
def cache_command(clear):
    """Manage DVCAP cache directory."""
    try:
        config = Config()
        cache_dir = config.downloads_path / 'dvcap'
        manager = DVCAPFileManager(cache_dir=cache_dir)
        
        if clear:
            # Get confirmation
            if click.confirm(f'Clear all cached files in {cache_dir}?'):
                count = manager.downloader.clear_cache()
                console.print(f"[green]✓ Cleared {count} files[/green]")
            return
        
        # Show cache contents
        cached = manager.list_cached_files()
        
        if not cached:
            console.print("[yellow]Cache is empty[/yellow]")
            console.print(f"Cache directory: {cache_dir}")
            return
        
        # Create table
        table = Table(title=f"Cached DVCAP Files ({len(cached)} files)")
        table.add_column("Filename", style="cyan")
        table.add_column("Size", style="green", justify="right")
        table.add_column("Modified", style="yellow")
        
        total_size = 0
        for file_path in cached:
            stat = file_path.stat()
            size_mb = stat.st_size / (1024 * 1024)
            total_size += size_mb
            
            from datetime import datetime
            mod_time = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M')
            
            table.add_row(
                file_path.name,
                f"{size_mb:.2f} MB",
                mod_time
            )
        
        console.print(table)
        console.print(f"\n[green]Total: {total_size:.2f} MB[/green]")
        console.print(f"Location: {cache_dir}")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@dvcap_cli.command(name='stats')
def stats_command():
    """Show statistics for cached DVCAP files."""
    try:
        config = Config()
        manager = DVCAPFileManager(cache_dir=config.downloads_path / 'dvcap')
        
        stats = manager.get_file_stats()
        
        if stats['total_files'] == 0:
            console.print("[yellow]No cached files[/yellow]")
            return
        
        # Create summary table
        table = Table(title="DVCAP Cache Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        
        table.add_row("Total Files", str(stats['total_files']))
        table.add_row("Total Size", f"{stats['total_size_mb']:.2f} MB")
        
        console.print(table)
        
        # Show file details
        if stats['files']:
            console.print("\n[cyan]Files:[/cyan]")
            files_table = Table()
            files_table.add_column("Name", style="cyan")
            files_table.add_column("Size", style="green", justify="right")
            files_table.add_column("Modified", style="yellow")
            
            for f in stats['files']:
                files_table.add_row(
                    f['name'],
                    f"{f['size_mb']:.2f} MB",
                    f['modified']
                )
            
            console.print(files_table)
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@dvcap_cli.command(name='head')
@click.argument('file', type=click.Path(exists=False), required=False)
@click.option('-n', '--lines', type=int, default=10, help='Number of rows to display')
@click.option('--columns', multiple=True, help='Specific columns to display')
def head_command(file, lines, columns):
    """
    Display the first N rows of a DVCAP CSV file.
    
    FILE can be either:
    - A relative filename (checks cache directory)
    - An absolute file path
    
    If FILE is omitted, shows the latest cached file.
    """
    try:
        config = Config()
        cache_dir = config.downloads_path / 'dvcap'
        
        # Determine file path
        if file:
            file_path = Path(file)
            if not file_path.is_absolute():
                # Check cache directory
                file_path = cache_dir / file
        else:
            # Use latest cached file
            manager = DVCAPFileManager(cache_dir=cache_dir)
            cached = manager.list_cached_files()
            if not cached:
                console.print("[yellow]No cached files found[/yellow]")
                return
            file_path = max(cached, key=lambda p: p.stat().st_mtime)
            console.print(f"[cyan]Using latest cached file: {file_path.name}[/cyan]\n")
        
        if not file_path.exists():
            console.print(f"[red]File not found: {file_path}[/red]")
            return
        
        # Read file
        df = pd.read_csv(file_path)
        
        # Filter columns if specified
        if columns:
            missing = [col for col in columns if col not in df.columns]
            if missing:
                console.print(f"[yellow]Warning: Columns not found: {', '.join(missing)}[/yellow]")
            available = [col for col in columns if col in df.columns]
            if available:
                df = df[available]
        
        # Get first N rows
        preview = df.head(lines)
        
        console.print(f"[cyan]File: {file_path.name}[/cyan]")
        console.print(f"[cyan]Rows: {len(df):,} | Columns: {len(df.columns)}[/cyan]\n")
        console.print(preview.to_string(index=False))
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@dvcap_cli.command(name='fields')
@click.argument('file', type=click.Path(exists=False), required=False)
def fields_command(file):
    """
    List all columns/fields in a DVCAP CSV file.
    
    FILE can be either:
    - A relative filename (checks cache directory)
    - An absolute file path
    
    If FILE is omitted, shows fields from the latest cached file.
    """
    try:
        config = Config()
        cache_dir = config.downloads_path / 'dvcap'
        
        # Determine file path
        if file:
            file_path = Path(file)
            if not file_path.is_absolute():
                # Check cache directory
                file_path = cache_dir / file
        else:
            # Use latest cached file
            manager = DVCAPFileManager(cache_dir=cache_dir)
            cached = manager.list_cached_files()
            if not cached:
                console.print("[yellow]No cached files found[/yellow]")
                return
            file_path = max(cached, key=lambda p: p.stat().st_mtime)
            console.print(f"[cyan]Using latest cached file: {file_path.name}[/cyan]\n")
        
        if not file_path.exists():
            console.print(f"[red]File not found: {file_path}[/red]")
            return
        
        # Read just the header
        df = pd.read_csv(file_path, nrows=0)
        
        # Create table
        table = Table(title=f"DVCAP CSV Fields - {file_path.name}")
        table.add_column("#", style="dim", justify="right")
        table.add_column("Column Name", style="cyan")
        table.add_column("Data Type", style="green")
        
        # Sample a few rows to determine types
        sample_df = pd.read_csv(file_path, nrows=100)
        
        for idx, col in enumerate(df.columns, 1):
            dtype = str(sample_df[col].dtype)
            table.add_row(str(idx), col, dtype)
        
        console.print(table)
        console.print(f"\n[green]Total: {len(df.columns)} columns[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@dvcap_cli.command(name='types')
def types_command():
    """Display information about DVCAP file types."""
    console.print("\n[bold]DVCAP File Types[/bold]\n")
    
    table = Table(title="File Types")
    table.add_column("Code", style="cyan", justify="center")
    table.add_column("Description", style="green")
    
    table.add_row("DVCRES", "Volume Cap Results (Double Volume Cap suspension data)")
    
    console.print(table)
    console.print("\n[dim]DVCAP files contain information about trading venue suspensions due to double volume cap mechanisms.[/dim]\n")
