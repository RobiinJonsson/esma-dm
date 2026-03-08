"""
Main CLI entry point for ESMA Data Manager.
"""

import click
from rich.console import Console

from .firds import firds_cli
from .fitrs import fitrs_cli
from .dvcap import dvcap_cli
from .benchmarks import benchmarks_cli
from .schema import schema_cli
from .db import db_cli

console = Console()


@click.group()
@click.version_option(version='0.3.0', prog_name='esma-dm')
def cli():
    """
    ESMA Data Manager - CLI for managing ESMA financial data.
    
    Provides commands for file management, database operations, and data analysis.
    """
    pass


# Register command groups
cli.add_command(firds_cli)
cli.add_command(fitrs_cli)
cli.add_command(dvcap_cli)
cli.add_command(benchmarks_cli)
cli.add_command(schema_cli)
cli.add_command(db_cli)


@cli.command()
def info():
    """Show package information."""
    console.print("\n[bold cyan]ESMA Data Manager[/bold cyan]", style="bold")
    console.print("Version: 0.3.0")
    console.print("\nA modular Python package for ESMA financial data")
    console.print("with utilities and validators.\n")
    console.print("[dim]Use 'esma-dm --help' for available commands.[/dim]\n")


if __name__ == '__main__':
    cli()
