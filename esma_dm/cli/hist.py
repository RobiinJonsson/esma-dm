"""
CLI commands for the FIRDS historical database (esma_hist).

Commands:
  hist init   -- load FULINS baseline into esma_hist.duckdb
  hist update -- apply DLTINS delta files for a date range or period
  hist status -- show current state of the history database
  hist query  -- point-in-time or current instrument lookup
"""

import click
from datetime import datetime
from typing import Optional

import pandas as pd
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table

from esma_dm.clients.history import HistoryClient

console = Console()


def _make_client(db_path: Optional[str] = None) -> HistoryClient:
    return HistoryClient(db_path=db_path)


@click.group(name="hist")
@click.option(
    "--db",
    "db_path",
    default=None,
    help="Override path to esma_hist.duckdb",
    envvar="ESMA_HIST_DB",
)
@click.pass_context
def hist_cli(ctx, db_path: Optional[str]):
    """FIRDS historical database — build and query the esma_hist database.

    Uses FULINS full files as a baseline and DLTINS delta files for daily
    updates, following ESMA Section 8 version management (Section 4.2 of
    ESMA65-8-5014).

    The database is stored in esma_hist.duckdb.  Use --db to override the path
    or set the ESMA_HIST_DB environment variable.

    \b
    Quick-start:
        esma-dm hist init               # load FULINS from cache
        esma-dm hist update --period week   # apply one week of deltas
        esma-dm hist status             # show database state
        esma-dm hist query GB00B1YW4409     # current state
        esma-dm hist query GB00B1YW4409 --date 2026-01-07  # point-in-time
    """
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = db_path


@hist_cli.command(name="init")
@click.option(
    "--asset",
    "asset_types",
    multiple=True,
    type=click.Choice(list("CDEFHIJORS"), case_sensitive=False),
    help="Asset type(s) to load.  Repeat for multiple.  Default: all found in cache.",
)
@click.option(
    "--download",
    is_flag=True,
    default=False,
    help="Download fresh FULINS files from ESMA before loading.",
)
@click.pass_context
def init_cmd(ctx, asset_types, download: bool):
    """Load FULINS baseline into the history database.

    Reads the latest cached FULINS CSV files for each requested asset type
    and inserts them as version 1 records into esma_hist.duckdb.  Already-loaded
    files are skipped automatically.

    \b
    Examples:
        esma-dm hist init
        esma-dm hist init --asset E --asset D
        esma-dm hist init --download
    """
    client = _make_client(ctx.obj.get("db_path"))
    types = list(asset_types) if asset_types else None

    console.print()
    console.print(Panel.fit(
        "[bold cyan]Initializing FIRDS history baseline[/bold cyan]\n"
        f"Asset types: {', '.join(types) if types else 'all found in cache'}\n"
        f"Download: {'yes' if download else 'no (use local cache)'}",
        border_style="cyan",
    ))

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Loading FULINS files...", total=None)
        result = client.init(asset_types=types, download=download)
        progress.update(task, description="Done.")

    if result.get("status") == "no_files":
        console.print(f"\n[yellow]No FULINS files found.[/yellow] {result.get('message','')}\n")
        console.print("Run [bold]esma-dm firds download --type FULINS --asset E[/bold] first.\n")
        return

    files = result.get("details", [])

    table = Table(title="FULINS Load Results", box=box.ROUNDED, show_lines=False)
    table.add_column("File", style="cyan", max_width=50)
    table.add_column("Date", style="yellow", width=12)
    table.add_column("Asset", style="green", width=6)
    table.add_column("ISINs", style="bold green", justify="right", width=12)
    table.add_column("Listings", style="bold cyan", justify="right", width=12)
    table.add_column("Status", style="magenta", width=14)

    for r in files:
        table.add_row(
            r.get("file", ""),
            r.get("date", ""),
            r.get("asset_type", ""),
            f"{r.get('isins_inserted', r.get('records_inserted', 0)):,}",
            f"{r.get('listings_inserted', 0):,}",
            r.get("status", ""),
        )

    console.print()
    console.print(table)
    console.print(
        f"\n[bold green]ISINs inserted:[/bold green] {result.get('total_isins_inserted', result.get('total_records_inserted', 0)):,}  "
        f"[bold cyan]Listings inserted:[/bold cyan] {result.get('total_listings_inserted', 0):,}\n"
    )


@hist_cli.command(name="update")
@click.option(
    "--period",
    type=click.Choice(["week", "month"], case_sensitive=False),
    default=None,
    help="Shortcut: 'week' (7 days) or 'month' (30 days) from last processed date.",
)
@click.option("--date-from", default=None, help="Explicit start date (YYYY-MM-DD).")
@click.option("--date-to", default=None, help="Explicit end date (YYYY-MM-DD).")
@click.option(
    "--full",
    "full_range",
    is_flag=True,
    default=False,
    help="Apply all available DLTINS from the FULINS baseline date to today.",
)
@click.option(
    "--no-download",
    is_flag=True,
    default=False,
    help="Do not download from ESMA; use only locally cached files.",
)
@click.option(
    "--asset",
    "asset_types",
    multiple=True,
    type=click.Choice(list("CDEFHIJORS"), case_sensitive=False),
    help="Informational only — DLTINS files contain all asset types.",
)
@click.pass_context
def update_cmd(ctx, period, date_from, date_to, full_range, no_download, asset_types):
    """Apply DLTINS delta files to the history database.

    Date range priority:
      1. --date-from / --date-to (explicit)
      2. --period week | month (relative to last processed date)
      3. --full (FULINS baseline date to today)
      4. Default: last processed date + 1 to today

    Already-applied files are always skipped.

    \b
    Examples:
        esma-dm hist update
        esma-dm hist update --period week
        esma-dm hist update --period month
        esma-dm hist update --date-from 2026-01-04 --date-to 2026-01-11
        esma-dm hist update --full --no-download
    """
    client = _make_client(ctx.obj.get("db_path"))

    # Resolve --full into explicit dates using baseline date
    if full_range and not date_from:
        status = client.status()
        date_from = status.get("baseline_date")
        if date_from:
            from datetime import date, timedelta
            from esma_dm.clients.history import _date_str, _parse_date
            date_from = _date_str(_parse_date(date_from) + timedelta(days=1))
        date_to = date_to or datetime.today().strftime("%Y-%m-%d")

    console.print()
    _print_update_plan(period, date_from, date_to, no_download)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Applying delta files...", total=None)
        result = client.update(
            asset_types=list(asset_types) if asset_types else None,
            date_from=date_from,
            date_to=date_to,
            period=period,
            download=not no_download,
        )
        progress.update(task, description="Done.")

    if result.get("status") == "no_files":
        console.print(f"\n[yellow]{result.get('message', 'No files found.')}[/yellow]\n")
        return

    details = result.get("details", [])
    applied = [d for d in details if d.get("status") == "applied"]
    skipped = [d for d in details if d.get("status") == "already_processed"]
    errors = [d for d in details if d.get("status") == "error"]

    if applied:
        table = Table(title="Applied Delta Files", box=box.ROUNDED, show_lines=False)
        table.add_column("File", style="cyan", max_width=45)
        table.add_column("Date", style="yellow", width=12)
        table.add_column("New", style="green", justify="right", width=8)
        table.add_column("Modified", style="blue", justify="right", width=10)
        table.add_column("Terminated", style="red", justify="right", width=12)
        table.add_column("Cancelled", style="magenta", justify="right", width=10)

        for r in applied:
            table.add_row(
                r.get("file", ""),
                r.get("date", ""),
                f"{r.get('new', 0):,}",
                f"{r.get('modified', 0):,}",
                f"{r.get('terminated', 0):,}",
                f"{r.get('cancelled', 0):,}",
            )
        console.print()
        console.print(table)

    console.print(
        f"\n[bold green]Files applied:[/bold green] {result['files_applied']}  "
        f"[dim]skipped: {result['files_skipped']}[/dim]"
    )
    console.print(
        f"[bold]Totals[/bold] — "
        f"NEW: [green]{result['new']:,}[/green]  "
        f"MODIFIED: [blue]{result['modified']:,}[/blue]  "
        f"TERMINATED: [red]{result['terminated']:,}[/red]  "
        f"CANCELLED: [magenta]{result['cancelled']:,}[/magenta]"
    )
    if errors:
        console.print(f"[bold red]Errors in {len(errors)} file(s)[/bold red]")
    console.print()


def _print_update_plan(period, date_from, date_to, no_download):
    if date_from or date_to:
        range_desc = f"{date_from or 'auto'} to {date_to or 'today'}"
    elif period:
        range_desc = f"{period} from last processed date"
    else:
        range_desc = "next day after last processed to today"

    console.print(Panel.fit(
        f"[bold cyan]Updating FIRDS history[/bold cyan]\n"
        f"Range: {range_desc}\n"
        f"Download from ESMA: {'no' if no_download else 'yes'}",
        border_style="cyan",
    ))


@hist_cli.command(name="status")
@click.pass_context
def status_cmd(ctx):
    """Show the current state of the history database.

    Displays baseline information (which FULINS files were loaded and when),
    delta processing coverage, and aggregate record counts.

    \b
    Example:
        esma-dm hist status
    """
    client = _make_client(ctx.obj.get("db_path"))
    info = client.status()

    console.print()

    # Overview panel
    active = info.get("instruments_active", 0)
    latest = info.get("instruments_latest", 0)
    hist_v = info.get("history_versions", 0)
    console.print(Panel.fit(
        f"[bold cyan]FIRDS History Database[/bold cyan]\n"
        f"Path: [dim]{info.get('db_path', 'N/A')}[/dim]\n\n"
        f"Active instruments: [bold green]{active:,}[/bold green]  "
        f"(latest: {latest:,})\n"
        f"Historical versions: [blue]{hist_v:,}[/blue]",
        border_style="cyan",
    ))

    # Baseline
    baseline_files = info.get("baseline_files", [])
    if baseline_files:
        btable = Table(title="Baseline (FULINS loads)", box=box.ROUNDED, show_lines=False)
        btable.add_column("Asset", style="green", width=6)
        btable.add_column("File Date", style="yellow", width=12)
        btable.add_column("Records", style="bold", justify="right", width=12)

        for bf in baseline_files:
            btable.add_row(
                str(bf.get("asset_type", "")),
                str(bf.get("file_date", "")),
                f"{bf.get('records_loaded', 0):,}",
            )
        console.print()
        console.print(btable)

    # Delta coverage
    if info.get("delta_files_applied", 0) > 0:
        dtable = Table(title="Delta Coverage", box=box.ROUNDED, show_lines=False)
        dtable.add_column("Metric", style="cyan")
        dtable.add_column("Value", style="bold", justify="right")

        dtable.add_row("Files applied", str(info.get("delta_files_applied", 0)))
        dtable.add_row("From date", str(info.get("delta_from_date", "N/A")))
        dtable.add_row("To date", str(info.get("delta_to_date", "N/A")))
        dtable.add_row("Total NEW", f"{info.get('total_new', 0):,}")
        dtable.add_row("Total MODIFIED", f"{info.get('total_modified', 0):,}")
        dtable.add_row("Total TERMINATED", f"{info.get('total_terminated', 0):,}")
        dtable.add_row("Total CANCELLED", f"{info.get('total_cancelled', 0):,}")

        console.print()
        console.print(dtable)
    else:
        console.print("\n[yellow]No delta files have been applied yet.[/yellow]")
        console.print("Run [bold]esma-dm hist update[/bold] to apply DLTINS files.\n")

    console.print()


@hist_cli.command(name="query")
@click.argument("isin")
@click.option(
    "--date",
    "as_of",
    default=None,
    help="Point-in-time date (YYYY-MM-DD).  Omit for current state.",
)
@click.option(
    "--history",
    "show_history",
    is_flag=True,
    default=False,
    help="Show all tracked versions for this ISIN.",
)
@click.pass_context
def query_cmd(ctx, isin: str, as_of: Optional[str], show_history: bool):
    """Look up an instrument in the history database.

    Returns the current state or (with --date) the version active on a
    specific date, per ESMA Section 9 query pattern.

    \b
    Examples:
        esma-dm hist query GB00B1YW4409
        esma-dm hist query GB00B1YW4409 --date 2026-01-07
        esma-dm hist query GB00B1YW4409 --history
    """
    client = _make_client(ctx.obj.get("db_path"))

    console.print()

    if show_history:
        df = client.version_history(isin)
        if df.empty:
            console.print(f"[yellow]No history found for ISIN {isin}[/yellow]\n")
            return

        htable = Table(
            title=f"Version history — {isin}",
            box=box.ROUNDED,
            show_lines=True,
        )
        for col in df.columns:
            htable.add_column(col, style="cyan")
        for _, row in df.iterrows():
            htable.add_row(*[str(v) if not (isinstance(v, float) and __import__('math').isnan(v)) else "" for v in row])
        console.print(htable)
        console.print()
        return

    result = client.query(isin, as_of=as_of)
    if not result:
        label = f"as of {as_of}" if as_of else "current state"
        console.print(f"[yellow]No record found for ISIN {isin} ({label})[/yellow]\n")
        return

    title = f"[bold cyan]{isin}[/bold cyan]"
    if as_of:
        title += f" — as of {as_of}"
    else:
        title += " — current state"

    table = Table(box=box.ROUNDED, show_header=False, show_lines=False)
    table.add_column("Field", style="dim", width=24)
    table.add_column("Value", style="bold")

    field_labels = {
        "isin": "ISIN",
        "cfi_code": "CFI Code",
        "instrument_type": "Asset Type",
        "full_name": "Full Name",
        "short_name": "Short Name",
        "issuer": "Issuer (LEI)",
        "currency": "Currency",
        "competent_authority": "Competent Authority",
        "valid_from_date": "Valid From",
        "valid_to_date": "Valid To",
        "latest_record_flag": "Latest",
        "record_type": "Record Type",
        "version_number": "Version",
        "source_file": "Source File",
    }

    for key, label in field_labels.items():
        val = result.get(key)
        if val is None:
            continue
        table.add_row(label, str(val))

    console.print(Panel(table, title=title, border_style="cyan"))
    console.print()
