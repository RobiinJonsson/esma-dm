"""
Benchmarks CLI commands for API access.

Provides command-line interface for querying ESMA Benchmarks data via API.
"""

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
import pandas as pd
import requests

from esma_dm.utils.constants import BENCHMARKS_ENTITIES_SOLR_URL, BENCHMARKS_BENCHMARKS_SOLR_URL
from esma_dm.utils import Utils

console = Console()


@click.group(name='benchmarks')
def benchmarks_cli():
    """
    Benchmarks API access commands.
    
    Query ESMA Benchmarks register for administrator and benchmark data.
    """
    pass


@benchmarks_cli.command(name='administrators')
@click.option('--country', help='Filter by country (e.g., UNITED KINGDOM, GERMANY)')
@click.option('--status', help='Filter by status (e.g., "Recognition under Art. 32", "Endorsement under Art. 33")')
@click.option('--limit', type=int, default=100, help='Maximum number of results')
@click.option('--json', 'output_json', is_flag=True, help='Output raw JSON for machine processing')
def administrators_command(country, status, limit, output_json):
    """
    Query benchmark administrators from ESMA register.
    
    Shows administrators authorized or registered under BMR (Benchmarks Regulation).
    """
    try:
        # Build query
        filters = []
        if country:
            filters.append(f'en_country:"{country}"')
        if status:
            filters.append(f'en_euEeaStatus:"{status}"')
        
        query = 'type_s:parent'
        if filters:
            query += ' AND ' + ' AND '.join(filters)
        
        wt_format = 'json' if output_json else 'xml'
        url = f"{BENCHMARKS_ENTITIES_SOLR_URL}?q={query}&rows={limit}&wt={wt_format}&indent=true"
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching administrators...", total=None)
            response = requests.get(url, timeout=15)
            progress.update(task, completed=True)
        
        if response.status_code != 200:
            console.print(f"[red]Error: HTTP {response.status_code}[/red]")
            return
        
        # Output raw JSON if requested
        if output_json:
            import json
            console.print(json.dumps(response.json(), indent=2))
            return
        
        # Parse response
        utils = Utils()
        df = utils.parse_xml_response(response)
        
        if df.empty:
            console.print("[yellow]No administrators found[/yellow]")
            return
        
        # Create table
        table = Table(title=f"Benchmark Administrators ({len(df)} found)")
        table.add_column("Name", style="cyan", max_width=40)
        table.add_column("Country", style="green")
        table.add_column("Status", style="yellow", max_width=30)
        table.add_column("LEI", style="blue", max_width=20)
        
        for _, row in df.iterrows():
            table.add_row(
                str(row.get('en_fullName', 'N/A')),
                str(row.get('en_country', 'N/A')),
                str(row.get('en_euEeaStatus', 'N/A')),
                str(row.get('en_lei', 'N/A')) if pd.notna(row.get('en_lei')) else 'N/A'
            )
        
        console.print(table)
        console.print(f"\n[green]✓ Found {len(df)} administrators[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@benchmarks_cli.command(name='search')
@click.argument('name')
@click.option('--limit', type=int, default=50, help='Maximum number of results')
@click.option('--json', 'output_json', is_flag=True, help='Output raw JSON for machine processing')
def search_command(name, limit, output_json):
    """
    Search for benchmarks by name.
    
    NAME: Search term (e.g., "MSCI", "LIBOR", "EONIA")
    """
    try:
        # Search in both benchmark names and administrator names
        query = f'bm_fullName:*{name}* OR en_fullName:*{name}*'
        wt_format = 'json' if output_json else 'xml'
        url = f"{BENCHMARKS_ENTITIES_SOLR_URL}?q={query}&rows={limit}&wt={wt_format}&indent=true"
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Searching for '{name}'...", total=None)
            response = requests.get(url, timeout=15)
            progress.update(task, completed=True)
        
        if response.status_code != 200:
            console.print(f"[red]Error: HTTP {response.status_code}[/red]")
            return
        
        # Output raw JSON if requested
        if output_json:
            import json
            console.print(json.dumps(response.json(), indent=2))
            return
        
        # Parse response
        utils = Utils()
        df = utils.parse_xml_response(response)
        
        if df.empty:
            console.print(f"[yellow]No results found for '{name}'[/yellow]")
            return
        
        # Separate administrators and benchmarks
        admins = df[df['type_s'] == 'parent']
        benchmarks = df[df['type_s'] == 'child']
        
        if not admins.empty:
            console.print(f"\n[bold]Administrators ({len(admins)}):[/bold]")
            admin_table = Table()
            admin_table.add_column("Name", style="cyan")
            admin_table.add_column("Country", style="green")
            admin_table.add_column("Status", style="yellow")
            
            for _, row in admins.iterrows():
                admin_table.add_row(
                    str(row.get('en_fullName', 'N/A')),
                    str(row.get('en_country', 'N/A')),
                    str(row.get('en_euEeaStatus', 'N/A'))
                )
            console.print(admin_table)
        
        if not benchmarks.empty:
            console.print(f"\n[bold]Benchmarks ({len(benchmarks)}):[/bold]")
            bench_table = Table()
            bench_table.add_column("Name", style="cyan", max_width=50)
            bench_table.add_column("Administrator", style="green", max_width=30)
            bench_table.add_column("Country", style="yellow")
            
            for _, row in benchmarks.iterrows():
                bench_table.add_row(
                    str(row.get('bm_fullName', 'N/A')),
                    str(row.get('bm_relatedAdministratorFullName', 'N/A')),
                    str(row.get('bm_administratorCountry', 'N/A'))
                )
            console.print(bench_table)
        
        console.print(f"\n[green]✓ Found {len(df)} results[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@benchmarks_cli.command(name='list')
@click.option('--status', help='Filter by BMR status (e.g., "Endorsement under Art. 33", "Recognition under Art. 32")')
@click.option('--country', help='Filter by administrator country')
@click.option('--administrator', help='Filter by administrator name')
@click.option('--isin', help='Filter by benchmark ISIN')
@click.option('--index-code', help='Filter by index code (e.g., EONA, EURI, LIBO)')
@click.option('--limit', type=int, default=100, help='Maximum number of results')
@click.option('--json', 'output_json', is_flag=True, help='Output raw JSON for machine processing')
def list_benchmarks(status, country, administrator, isin, index_code, limit, output_json):
    """
    Query third-country benchmarks from ESMA register.
    
    Shows benchmarks compliant with the Benchmarks Regulation (BMR),
    including administrator details and endorsing entities.
    """
    try:
        # Build query
        filters = []
        if status:
            filters.append(f'bm_euEeaStatus:"{status}"')
        if country:
            filters.append(f'bm_administratorCountry:"{country}"')
        if administrator:
            filters.append(f'bm_relatedAdministratorFullName:*{administrator}*')
        if isin:
            filters.append(f'bm_isin:"{isin}"')
        if index_code:
            filters.append(f'bm_indexCode:"{index_code}"')
        
        query = '*:*' if not filters else ' AND '.join(filters)
        wt_format = 'json' if output_json else 'xml'
        url = f"{BENCHMARKS_BENCHMARKS_SOLR_URL}?q={query}&rows={limit}&wt={wt_format}&indent=true"
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching benchmarks...", total=None)
            response = requests.get(url, timeout=15)
            progress.update(task, completed=True)
        
        if response.status_code != 200:
            console.print(f"[red]Error: HTTP {response.status_code}[/red]")
            return
        
        # Output raw JSON if requested
        if output_json:
            import json
            console.print(json.dumps(response.json(), indent=2))
            return
        
        # Parse response
        utils = Utils()
        df = utils.parse_xml_response(response)
        
        if df.empty:
            console.print("[yellow]No benchmarks found[/yellow]")
            return
        
        # Create table
        table = Table(title=f"Third-Country Benchmarks ({len(df)} found)")
        table.add_column("Benchmark Name", style="cyan", max_width=40)
        table.add_column("ISIN", style="blue", max_width=12)
        table.add_column("Administrator", style="green", max_width=30)
        table.add_column("Country", style="yellow")
        table.add_column("Status", style="magenta", max_width=25)
        
        for _, row in df.iterrows():
            table.add_row(
                str(row.get('bm_fullName', 'N/A')),
                str(row.get('bm_isin', 'N/A')),
                str(row.get('bm_relatedAdministratorFullName', 'N/A')),
                str(row.get('bm_administratorCountry', 'N/A')),
                str(row.get('bm_euEeaStatus', 'N/A'))
            )
        
        console.print(table)
        console.print(f"\n[green]✓ Found {len(df)} benchmarks[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@benchmarks_cli.command(name='endorsed')
@click.option('--endorsing-country', help='Filter by endorsing entity country')
@click.option('--limit', type=int, default=100, help='Maximum number of results')
@click.option('--json', 'output_json', is_flag=True, help='Output raw JSON for machine processing')
def endorsed_benchmarks(endorsing_country, limit, output_json):
    """
    Query benchmarks endorsed under Article 33 of BMR.
    
    Shows endorsed benchmarks with details of endorsing entities
    (administrators or supervised entities located in EU/EEA).
    """
    try:
        # Build query for endorsed benchmarks
        filters = ['bm_euEeaStatus:"Endorsement under Art. 33"']
        if endorsing_country:
            filters.append(f'bm_endorsingEntityCountry:"{endorsing_country}"')
        
        query = ' AND '.join(filters)
        wt_format = 'json' if output_json else 'xml'
        url = f"{BENCHMARKS_BENCHMARKS_SOLR_URL}?q={query}&sort=bm_fullName asc&rows={limit}&wt={wt_format}&indent=true"
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching endorsed benchmarks...", total=None)
            response = requests.get(url, timeout=15)
            progress.update(task, completed=True)
        
        if response.status_code != 200:
            console.print(f"[red]Error: HTTP {response.status_code}[/red]")
            return
        
        # Output raw JSON if requested
        if output_json:
            import json
            console.print(json.dumps(response.json(), indent=2))
            return
        
        # Parse response
        utils = Utils()
        df = utils.parse_xml_response(response)
        
        if df.empty:
            console.print("[yellow]No endorsed benchmarks found[/yellow]")
            return
        
        # Create table
        table = Table(title=f"Endorsed Benchmarks - Article 33 ({len(df)} found)")
        table.add_column("Benchmark", style="cyan", max_width=35)
        table.add_column("Administrator", style="green", max_width=25)
        table.add_column("Endorsing Entity", style="yellow", max_width=30)
        table.add_column("Endorsing Country", style="blue")
        
        for _, row in df.iterrows():
            table.add_row(
                str(row.get('bm_fullName', 'N/A')),
                str(row.get('bm_relatedAdministratorFullName', 'N/A')),
                str(row.get('bm_endorsingEntityFullName', 'N/A')),
                str(row.get('bm_endorsingEntityCountry', 'N/A'))
            )
        
        console.print(table)
        console.print(f"\n[green]✓ Found {len(df)} endorsed benchmarks[/green]")
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        raise click.Abort()


@benchmarks_cli.command(name='statuses')
def statuses_command():
    """
    Display available BMR (Benchmarks Regulation) status types.
    
    Shows the predefined status values that can be used for filtering.
    """
    console.print("\n[bold]BMR Status Types[/bold]\n")
    
    table = Table(title="Benchmarks Regulation Status Values")
    table.add_column("Status", style="cyan")
    table.add_column("Article", style="green", justify="center")
    table.add_column("Description", style="yellow")
    
    statuses = [
        ("Equivalence under Art. 30", "Art. 30", "Third-country regime deemed equivalent"),
        ("Recognition under Art. 32", "Art. 32", "Third-country administrator recognized"),
        ("Endorsement under Art. 33", "Art. 33", "Benchmark endorsed by EU/EEA entity"),
        ("Authorisation under Art. 34", "Art. 34", "Administrator authorized in EU/EEA"),
        ("Registration under Art. 34", "Art. 34", "Administrator registered in EU/EEA"),
    ]
    
    for status, article, desc in statuses:
        table.add_row(status, article, desc)
    
    console.print(table)
    
    console.print("\n[dim]Usage examples:[/dim]")
    console.print('  esma-dm benchmarks administrators --status "Recognition under Art. 32"')
    console.print('  esma-dm benchmarks list --status "Endorsement under Art. 33"')
    console.print('  esma-dm benchmarks endorsed\n')


@benchmarks_cli.command(name='info')
def info_command():
    """
    Display field reference for benchmark data.
    
    Shows available fields and common index codes.
    """
    console.print("\n[bold]Benchmark Data Fields[/bold]\n")
    
    # Administrator fields
    admin_table = Table(title="Administrator Fields (prefix: en_)")
    admin_table.add_column("Field", style="cyan")
    admin_table.add_column("Description", style="green")
    
    admin_fields = [
        ("en_esmaId", "Administrator ESMA ID (unique identifier)"),
        ("en_fullName", "Administrator full name"),
        ("en_lei", "Legal Entity Identifier (LEI)"),
        ("en_country", "Administrator location country"),
        ("en_supervisingAuthority", "Supervising authority"),
        ("en_euEeaStatus", "BMR status (Art. 30, 32, 33, 34)"),
    ]
    
    for field, desc in admin_fields:
        admin_table.add_row(field, desc)
    
    console.print(admin_table)
    
    # Benchmark fields
    bench_table = Table(title="\nBenchmark Fields (prefix: bm_)")
    bench_table.add_column("Field", style="cyan")
    bench_table.add_column("Description", style="green")
    
    bench_fields = [
        ("bm_esmaId", "Benchmark ESMA ID (unique identifier)"),
        ("bm_fullName", "Benchmark full name"),
        ("bm_isin", "Benchmark ISIN code"),
        ("bm_indexCode", "Index code (EONA, EURI, LIBO, etc.)"),
        ("bm_euEeaStatus", "BMR status"),
        ("bm_relatedAdministratorFullName", "Administrator name"),
        ("bm_endorsingEntityFullName", "Endorsing entity (Art. 33)"),
    ]
    
    for field, desc in bench_fields:
        bench_table.add_row(field, desc)
    
    console.print(bench_table)
    
    # Common index codes
    console.print("\n[bold]Common Index Codes[/bold]")
    codes_table = Table()
    codes_table.add_column("Code", style="cyan", justify="center")
    codes_table.add_column("Description", style="green")
    
    index_codes = [
        ("EONA", "EONIA - Euro Overnight Index Average"),
        ("EONS", "EONIA Swap"),
        ("EURI", "EURIBOR - Euro Interbank Offered Rate"),
        ("LIBI", "LIBOR - ICE"),
        ("LIBO", "LIBOR - London Interbank Offered Rate"),
        ("SONI", "SONIA - Sterling Overnight Index Average"),
        ("BBSW", "Bank Bill Swap Rate"),
        ("TIBO", "TIBOR - Tokyo Interbank Offered Rate"),
    ]
    
    for code, desc in index_codes:
        codes_table.add_row(code, desc)
    
    console.print(codes_table)
    console.print()
