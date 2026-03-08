"""
FIRDS file management commands for ESMA Data Manager CLI.
"""

import click
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich import box

from esma_dm.file_manager import FIRDSFileManager, FileType, AssetType
from esma_dm.config import Config

console = Console()


@click.group(name='firds')
def firds_cli():
    """FIRDS reference data — file management and instrument lookup."""
    pass


@firds_cli.command(name='list')
@click.option('--type', 'file_type', 
              type=click.Choice(['FULINS', 'DLTINS'], case_sensitive=False),
              help='Filter by file type (FULINS=full, DLTINS=delta)')
@click.option('--asset', 'asset_type',
              type=click.Choice(['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S'], case_sensitive=False),
              help='Filter by asset type')
@click.option('--date-from', 
              help='Start date (YYYY-MM-DD)')
@click.option('--date-to',
              help='End date (YYYY-MM-DD)')
@click.option('--limit', default=None, type=int,
              help='Maximum number of files to list (omit for all files)')
@click.option('--fetch-all', is_flag=True, default=False,
              help='Fetch all available files using pagination')
def list_files(file_type: Optional[str], asset_type: Optional[str], 
               date_from: Optional[str], date_to: Optional[str], 
               limit: Optional[int], fetch_all: bool):
    """
    List available files from ESMA FIRDS register.
    
    By default, fetches all available files using pagination. Use --limit to restrict results.
    
    Examples:
    
        esma-dm firds list --type FULINS --asset E
        
        esma-dm firds list --date-from 2026-01-01 --limit 50
        
        esma-dm firds list --asset E --fetch-all
    """
    try:
        console.print("\n[bold cyan]Fetching file list from ESMA...[/bold cyan]\n")
        
        # Initialize file manager with date range
        config = Config()
        date_to_use = date_to or datetime.today().strftime('%Y-%m-%d')
        date_from_use = date_from or '2018-01-01'
        
        cache_dir = config.downloads_path / 'firds'
        manager = FIRDSFileManager(
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
            
            df = manager.list_files(
                file_type=file_type,
                asset_type=asset_type,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                fetch_all=should_fetch_all
            )
        
        if df.empty:
            console.print("[yellow]No files found matching the criteria.[/yellow]\n")
            return
        
        # Create rich table
        table = Table(title="ESMA FIRDS Files", box=box.ROUNDED, show_lines=False)
        table.add_column("File Name", style="cyan", no_wrap=False, max_width=40)
        table.add_column("Type", style="magenta", width=8)
        table.add_column("Asset", style="green", width=6)
        table.add_column("Date", style="yellow", width=10)
        table.add_column("Part", style="blue", width=8)
        table.add_column("Size", style="dim", justify="right", width=12)
        
        for _, row in df.iterrows():
            file_name = str(row.get('file_name', 'N/A'))
            f_type = str(row.get('file_type', 'N/A')) if pd.notna(row.get('file_type')) else 'N/A'
            a_type = str(row.get('asset_type', 'N/A')) if pd.notna(row.get('asset_type')) else 'N/A'
            f_date = str(row.get('file_date', 'N/A')) if pd.notna(row.get('file_date')) else 'N/A'
            
            part_num = row.get('part_number')
            total_parts = row.get('total_parts')
            if pd.notna(part_num) and pd.notna(total_parts):
                part_info = f"{int(part_num)}/{int(total_parts)}"
            else:
                part_info = 'N/A'
            
            size = str(row.get('file_size', 'N/A'))
            
            table.add_row(file_name, f_type, a_type, f_date, part_info, size)
        
        console.print(table)
        console.print(f"\n[bold green]Total files: {len(df)}[/bold green]")
        
        # Show filtering info
        if file_type or asset_type:
            filters = []
            if file_type:
                filters.append(f"type={file_type}")
            if asset_type:
                filters.append(f"asset={asset_type}")
            console.print(f"[dim]Filtered by: {', '.join(filters)}[/dim]")
        
        console.print()
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@firds_cli.command(name='download')
@click.option('--type', 'file_type',
              type=click.Choice(['FULINS', 'DLTINS'], case_sensitive=False),
              default='FULINS',
              show_default=True,
              help='File type to download')
@click.option('--asset', 'asset_type',
              type=click.Choice(['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S'], case_sensitive=False),
              required=True,
              help='Asset type to download')
@click.option('--update/--no-update', default=False,
              help='Force fresh download (ignore cache)')
def download_files(file_type: str, asset_type: str, update: bool):
    """
    Download latest files from ESMA FIRDS register.
    
    Examples:
    
        esma-dm firds download --asset E
        
        esma-dm firds download --type DLTINS --asset E
        
        esma-dm firds download --asset D --update
    """
    try:
        console.print("\n[bold cyan]Downloading files from ESMA...[/bold cyan]\n")
        
        # Initialize file manager
        config = Config()
        cache_dir = config.downloads_path / 'firds'
        manager = FIRDSFileManager(
            cache_dir=cache_dir,
            date_from='2018-01-01',
            date_to=datetime.today().strftime('%Y-%m-%d'),
            config=config
        )
        
        # Download files
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Downloading...", total=None)
            
            if file_type.upper() == 'FULINS':
                result = manager.download_latest_full_files(
                    asset_type=asset_type,
                    update=update
                )
            else:
                console.print("[yellow]Delta file downloads coming soon![/yellow]\n")
                return
        
        if result:
            console.print(f"\n[bold green]Success![/bold green] Downloaded {len(result)} file(s)")
            
            # Show downloaded files
            table = Table(title="Downloaded Files", box=box.ROUNDED)
            table.add_column("File Name", style="cyan")
            table.add_column("Size", style="blue", justify="right")
            
            for file_path in result:
                size = file_path.stat().st_size
                size_mb = f"{size / (1024*1024):.2f} MB"
                table.add_row(file_path.name, size_mb)
            
            console.print(table)
        else:
            console.print("[yellow]No files downloaded.[/yellow]")
        
        console.print()
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        import traceback
        traceback.print_exc()
        raise click.Abort()


@firds_cli.command(name='cache')
@click.option('--asset', 'asset_type',
              type=click.Choice(['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S'], case_sensitive=False),
              help='Filter by asset type')
@click.option('--type', 'file_type',
              type=click.Choice(['FULINS', 'DLTINS'], case_sensitive=False),
              help='Filter by file type')
def list_cache(asset_type: Optional[str], file_type: Optional[str]):
    """
    List files in local cache directory.
    
    Examples:
    
        esma-dm firds cache
        
        esma-dm firds cache --asset E
        
        esma-dm firds cache --type FULINS --asset D
    """
    try:
        config = Config()
        cache_dir = config.downloads_path / 'firds'
        
        if not cache_dir.exists():
            console.print(f"\n[yellow]Cache directory does not exist:[/yellow] {cache_dir}\n")
            return
        
        # Use file manager to list cached files
        manager = FIRDSFileManager(
            cache_dir=cache_dir,
            date_from='2018-01-01',
            date_to=datetime.today().strftime('%Y-%m-%d')
        )
        
        cached_files = manager.list_cached_files(
            file_type=file_type,
            asset_type=asset_type
        )
        
        if not cached_files:
            console.print("\n[yellow]No files match the filter criteria.[/yellow]\n")
            return
        
        # Create rich table
        table = Table(title=f"Cached Files ({cache_dir})", box=box.ROUNDED, show_lines=False)
        table.add_column("#", style="dim", width=4)
        table.add_column("File Name", style="cyan", no_wrap=False)
        table.add_column("Type", style="magenta", width=8)
        table.add_column("Asset", style="green", width=6)
        table.add_column("Size", style="blue", justify="right", width=12)
        table.add_column("Modified", style="yellow", width=16)
        
        for idx, file_path in enumerate(sorted(cached_files), 1):
            size = file_path.stat().st_size
            size_mb = f"{size / (1024*1024):.2f} MB"
            modified = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M')
            
            # Extract type and asset from filename
            name = file_path.name
            ftype = 'FULINS' if 'FULINS' in name else 'DLTINS' if 'DLTINS' in name else 'OTHER'
            
            # Extract asset type (single char after first underscore)
            import re
            match = re.search(r'_([A-Z])_', name)
            atype = match.group(1) if match else '?'
            
            table.add_row(str(idx), file_path.name, ftype, atype, size_mb, modified)
        
        console.print("\n", table, "\n")
        console.print(f"[bold green]Total cached files: {len(cached_files)}[/bold green]\n")
        
        # Show statistics
        stats = manager.get_file_stats(file_type=file_type, asset_type=asset_type)
        if stats['by_type'] or stats['by_asset']:
            console.print("[cyan]Statistics:[/cyan]")
            if stats['by_type']:
                console.print(f"  By type: {dict(stats['by_type'])}")
            if stats['by_asset']:
                console.print(f"  By asset: {dict(stats['by_asset'])}")
            console.print(f"  Total size: {stats['total_size_mb']} MB\n")
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@firds_cli.command(name='fields')
@click.argument('file_path', type=click.Path())
def list_fields(file_path: str):
    """
    List all field names (columns) in a CSV file.
    
    FILE_PATH: Path to the CSV file (can be in cache or absolute path)
    
    Examples:
    
        esma-dm firds fields downloads/data/firds/FULINS_E_2026-01-15.csv
    """
    try:
        file_path_obj = Path(file_path)
        
        # If relative path, try cache directory
        if not file_path_obj.is_absolute():
            config = Config()
            cache_path = config.downloads_path / 'firds' / file_path
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


@firds_cli.command(name='head')
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
    
        esma-dm firds head downloads/data/firds/FULINS_E_2026-01-15.csv
        
        esma-dm firds head FULINS_E_2026-01-15.csv --rows 20
        
        esma-dm firds head FULINS_E_2026-01-15.csv --columns "FinInstrmGnlAttrbts_Id,FinInstrmGnlAttrbts_FullNm,FinInstrmGnlAttrbts_ClssfctnTp"
    """
    try:
        file_path_obj = Path(file_path)
        
        # If relative path, try cache directory
        if not file_path_obj.is_absolute():
            config = Config()
            cache_path = config.downloads_path / 'firds' / file_path
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


@firds_cli.command(name='types')
def list_types():
    """
    List available file types and asset types.
    
    Shows all supported FIRDS types and their descriptions.
    
    Example:
    
        esma-dm firds types
    """
    try:
        console.print("\n[bold cyan]FIRDS File Types and Asset Types[/bold cyan]\n")
        
        # File Types
        file_table = Table(title="File Types", box=box.ROUNDED)
        file_table.add_column("Code", style="cyan", width=10)
        file_table.add_column("Description", style="white")
        
        file_table.add_row("FULINS", "Full Instrument Reference Data (snapshots)")
        file_table.add_row("DLTINS", "Delta Instrument Data (daily changes)")
        file_table.add_row("FULCAN", "Full Cancellation Files")
        
        console.print(file_table)
        console.print()
        
        # Asset Types
        asset_table = Table(title="Asset Types (CFI First Character)", box=box.ROUNDED)
        asset_table.add_column("Code", style="green", width=6)
        asset_table.add_column("Description", style="white")
        
        for asset in AssetType:
            desc = {
                'C': 'Collective Investment Vehicles',
                'D': 'Debt Instruments (Bonds, Notes)',
                'E': 'Equities (Shares, Units)',
                'F': 'Futures',
                'H': 'Rights & Warrants',
                'I': 'Options',
                'J': 'Strategies & Multi-leg',
                'O': 'Others (Miscellaneous)',
                'R': 'Referential Instruments',
                'S': 'Swaps'
            }.get(asset.value, asset.name)
            
            asset_table.add_row(asset.value, desc)
        
        console.print(asset_table)
        console.print()
        
        console.print("[dim]Use these codes with --type and --asset options.[/dim]\n")
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@firds_cli.command(name='reference')
@click.argument('isin')
@click.option('--mode', default='current', type=click.Choice(['current', 'history'], case_sensitive=False),
              help='Database mode (default: current)')
def reference_lookup(isin: str, mode: str):
    """
    Look up an instrument by ISIN.

    Displays master fields plus asset-specific detail columns.

    Examples:

        esma-dm firds reference SE0000242455

        esma-dm firds reference US0378331005
    """
    try:
        from esma_dm.clients.firds import FIRDSClient

        firds = FIRDSClient(mode=mode)
        instrument = firds.data_store.get_instrument(isin.upper())

        if instrument is None:
            console.print(f"\n[yellow]No instrument found for ISIN:[/yellow] {isin.upper()}\n")
            return

        console.print()

        # Master fields
        master_table = Table(title=f"Instrument  {isin.upper()}", box=box.ROUNDED, show_header=True)
        master_table.add_column("Field", style="cyan", width=28)
        master_table.add_column("Value", style="white")

        skip_fields = {'asset_specific', 'cfi_classification'}
        for key, value in instrument.items():
            if key in skip_fields:
                continue
            master_table.add_row(key, str(value) if value is not None else "[dim]—[/dim]")

        console.print(master_table)

        # CFI classification
        if instrument.get('cfi_classification'):
            cfi = instrument['cfi_classification']
            cfi_table = Table(title="CFI Classification", box=box.SIMPLE, show_header=True)
            cfi_table.add_column("Dimension", style="magenta", width=18)
            cfi_table.add_column("Value", style="white")
            for k, v in cfi.items():
                cfi_table.add_row(k, str(v) if v is not None else "[dim]—[/dim]")
            console.print(cfi_table)

        # Asset-specific detail
        if instrument.get('asset_specific'):
            detail = instrument['asset_specific']
            detail_table = Table(title="Asset-Specific Fields", box=box.SIMPLE, show_header=True)
            detail_table.add_column("Field", style="green", width=32)
            detail_table.add_column("Value", style="white")
            for k, v in detail.items():
                if v is not None and str(v).strip() not in ('', 'None', 'nan'):
                    detail_table.add_row(k, str(v))
            console.print(detail_table)

        console.print()

    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@firds_cli.command(name='search')
@click.argument('query')
@click.option('--asset', 'asset_type',
              type=click.Choice(['C', 'D', 'E', 'F', 'H', 'I', 'J', 'O', 'R', 'S'], case_sensitive=False),
              help='Filter by asset type')
@click.option('--limit', default=20, show_default=True, help='Maximum number of results')
@click.option('--mode', default='current', type=click.Choice(['current', 'history'], case_sensitive=False),
              help='Database mode (default: current)')
def search_instruments(query: str, asset_type: Optional[str], limit: int, mode: str):
    """
    Search instruments by name or ISIN prefix.

    Matches against full_name (case-insensitive) and ISIN.

    Examples:

        esma-dm firds search "apple"

        esma-dm firds search "volkswagen" --asset E --limit 10

        esma-dm firds search "US0378"
    """
    try:
        from esma_dm.clients.firds import FIRDSClient

        firds = FIRDSClient(mode=mode)
        con = firds.data_store.con

        like_pattern = f"%{query.upper()}%"
        params = [like_pattern, like_pattern]

        asset_filter = ""
        if asset_type:
            asset_filter = "AND instrument_type = ?"
            params.append(asset_type.upper())

        params.append(limit)

        sql = f"""
            SELECT isin, instrument_type, cfi_code, full_name, currency
            FROM instruments
            WHERE (UPPER(isin) LIKE ? OR UPPER(full_name) LIKE ?)
            {asset_filter}
            ORDER BY full_name
            LIMIT ?
        """

        df = con.execute(sql, params).fetchdf()

        if df.empty:
            console.print(f"\n[yellow]No results for:[/yellow] {query}\n")
            return

        console.print(f"\n[bold cyan]Results for '{query}'[/bold cyan]  ({len(df)} shown)\n")

        result_table = Table(box=box.ROUNDED, show_lines=False)
        result_table.add_column("ISIN", style="cyan", width=14)
        result_table.add_column("Type", style="magenta", width=6)
        result_table.add_column("CFI", style="yellow", width=8)
        result_table.add_column("Name", style="white")
        result_table.add_column("CCY", style="green", width=5)

        for _, row in df.iterrows():
            result_table.add_row(
                row['isin'],
                str(row['instrument_type']) if row['instrument_type'] else '',
                str(row['cfi_code']) if row['cfi_code'] else '',
                str(row['full_name']) if row['full_name'] else '',
                str(row['currency']) if row['currency'] else '',
            )

        console.print(result_table)
        console.print()

    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()


@firds_cli.command(name='stats')
def show_stats():
    """
    Show statistics about cached files.
    
    Displays breakdown by file type and asset type.
    
    Example:
    
        esma-dm firds stats
    """
    try:
        config = Config()
        cache_dir = config.downloads_path / 'firds'
        
        if not cache_dir.exists():
            console.print(f"\n[yellow]Cache directory does not exist:[/yellow] {cache_dir}\n")
            return
        
        manager = FIRDSFileManager(
            cache_dir=cache_dir,
            date_from='2018-01-01',
            date_to=datetime.today().strftime('%Y-%m-%d')
        )
        
        stats = manager.get_file_stats()
        
        if stats['total_files'] == 0:
            console.print("\n[yellow]No cached files found.[/yellow]\n")
            return
        
        console.print("\n[bold cyan]Cache Statistics[/bold cyan]\n")
        console.print(f"[green]Total Files:[/green] {stats['total_files']}")
        console.print(f"[blue]Total Size:[/blue] {stats['total_size_mb']} MB\n")
        
        if stats['by_type']:
            type_table = Table(title="Files by Type", box=box.ROUNDED)
            type_table.add_column("Type", style="magenta")
            type_table.add_column("Count", style="cyan", justify="right")
            
            for ftype, count in sorted(stats['by_type'].items()):
                type_table.add_row(ftype, str(count))
            
            console.print(type_table)
            console.print()
        
        if stats['by_asset']:
            asset_table = Table(title="Files by Asset Type", box=box.ROUNDED)
            asset_table.add_column("Asset", style="green")
            asset_table.add_column("Count", style="cyan", justify="right")
            
            for atype, count in sorted(stats['by_asset'].items()):
                asset_table.add_row(atype, str(count))
            
            console.print(asset_table)
            console.print()
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}\n")
        raise click.Abort()
