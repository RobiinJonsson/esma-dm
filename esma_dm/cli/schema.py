"""
CLI commands for inspecting ESMA data model schemas.

Shows field definitions, source CSV column names, types, and descriptions
for FIRDS reference data, FITRS transparency data, CFI taxonomy, and enums.
"""
import click
from rich.console import Console
from rich.table import Table
from rich import box

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TYPE_COLORS = {
    'str': 'green',
    'float': 'yellow',
    'int': 'yellow',
    'bool': 'cyan',
    'date': 'magenta',
}


def _type_markup(t: str) -> str:
    color = TYPE_COLORS.get(t, 'white')
    return f'[{color}]{t}[/{color}]'


def _render_schema(title: str, subtitle: str, schema: dict, show_source: bool = False) -> None:
    """Render a schema dict as a Rich table."""
    console.print()
    console.print(f'[bold cyan]{title}[/bold cyan]')
    if subtitle:
        console.print(f'[dim]{subtitle}[/dim]')
    console.print()

    columns = ['Field', 'Type', 'Description']
    if show_source:
        columns = ['Field', 'Type', 'CSV Column', 'Description']

    table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style='bold')
    for col in columns:
        table.add_column(col)

    for field_name, meta in schema.items():
        ftype = meta.get('type', '')
        desc = meta.get('description', '')
        req = meta.get('required', False)
        field_fmt = f'[bold]{field_name}[/bold]' if req else field_name
        if show_source:
            source = meta.get('source', '')
            table.add_row(field_fmt, _type_markup(ftype), f'[dim]{source}[/dim]', desc)
        else:
            table.add_row(field_fmt, _type_markup(ftype), desc)

    console.print(table)


# ---------------------------------------------------------------------------
# Main group
# ---------------------------------------------------------------------------

@click.group(name='schema')
def schema_cli():
    """Inspect ESMA data model schemas and field definitions."""
    pass


# ---------------------------------------------------------------------------
# firds
# ---------------------------------------------------------------------------

FIRDS_ASSET_CHOICES = click.Choice(
    ['base', 'trading-venue', 'technical', 'equity', 'debt', 'derivative', 'option', 'future'],
    case_sensitive=False,
)


@schema_cli.command(name='firds')
@click.option('--asset', type=FIRDS_ASSET_CHOICES, default=None,
              help='Show schema for a specific asset class or sub-model.')
def firds_schema(asset: str | None):
    """Show FIRDS reference data field schemas.

    Without --asset, shows the base Instrument schema shared by all asset types.
    Use --asset to inspect a specific sub-model.

    \b
    Available models:
      base          Base Instrument (shared by all asset types)
      trading-venue TradingVenueAttributes (nested in every instrument)
      technical     TechnicalAttributes (nested in every instrument)
      equity        EquityInstrument (CFI E*)
      debt          DebtInstrument (CFI D*)
      derivative    DerivativeInstrument (CFI F/H/I/J/S*)
      option        OptionAttributes (nested in DerivativeInstrument)
      future        FutureAttributes (nested in DerivativeInstrument)
    """
    from esma_dm.models.base import Instrument, TradingVenueAttributes, TechnicalAttributes
    from esma_dm.models.equity import EquityInstrument
    from esma_dm.models.debt import DebtInstrument
    from esma_dm.models.derivative import DerivativeInstrument, OptionAttributes, FutureAttributes

    target = (asset or 'base').lower()

    if target == 'base':
        _render_schema(
            'FIRDS — Base Instrument',
            'Common fields across all asset types (Instrument dataclass)',
            Instrument.get_schema(),
        )
    elif target == 'trading-venue':
        _render_schema(
            'FIRDS — TradingVenueAttributes',
            'Nested in every Instrument as .trading_venue',
            TradingVenueAttributes.get_schema(),
        )
    elif target == 'technical':
        _render_schema(
            'FIRDS — TechnicalAttributes',
            'Nested in every Instrument as .technical',
            TechnicalAttributes.get_schema(),
        )
    elif target == 'equity':
        schema = {**Instrument.get_schema(), **EquityInstrument.get_schema()}
        _render_schema(
            'FIRDS — EquityInstrument (CFI E*)',
            'Equities, ETFs, structured equity instruments',
            schema,
        )
    elif target == 'debt':
        schema = {**Instrument.get_schema(), **DebtInstrument.get_schema()}
        _render_schema(
            'FIRDS — DebtInstrument (CFI D*)',
            'Bonds, notes, money market instruments',
            schema,
        )
    elif target == 'derivative':
        schema = {**Instrument.get_schema(), **DerivativeInstrument.get_schema()}
        _render_schema(
            'FIRDS — DerivativeInstrument (CFI F/H/I/J/S*)',
            'Futures, options, swaps, forwards, non-standard derivatives',
            schema,
        )
    elif target == 'option':
        _render_schema(
            'FIRDS — OptionAttributes',
            'Nested in DerivativeInstrument as .option_attrs  (CFI H*, O*)',
            OptionAttributes.get_schema(),
        )
    elif target == 'future':
        _render_schema(
            'FIRDS — FutureAttributes',
            'Nested in DerivativeInstrument as .future_attrs  (CFI F*)',
            FutureAttributes.get_schema(),
        )

    console.print('[dim]Bold field names are required.[/dim]\n')


# ---------------------------------------------------------------------------
# transparency
# ---------------------------------------------------------------------------

@schema_cli.command(name='transparency')
@click.option('--type', 'record_type',
              type=click.Choice(['equity', 'non-equity'], case_sensitive=False),
              default=None,
              help='Show equity (FULECR) or non-equity (FULNCR) schema.')
def transparency_schema(record_type: str | None):
    """Show FITRS transparency record field schemas.

    \b
    Available schemas:
      equity      EquityTransparencyRecord — FULECR / DLTECR data
      non-equity  NonEquityTransparencyRecord — FULNCR / DLTNCR data

    The CSV column name each field is sourced from is shown in the
    'CSV Column' column.
    """
    from esma_dm.models.transparency import EquityTransparencyRecord, NonEquityTransparencyRecord

    target = (record_type or '').lower()

    if target in ('equity', ''):
        _render_schema(
            'FITRS — EquityTransparencyRecord (FULECR / DLTECR)',
            'ISIN-level equity transparency thresholds — CFI E, C, R asset types',
            EquityTransparencyRecord.get_schema(),
            show_source=True,
        )

    if target in ('non-equity', ''):
        _render_schema(
            'FITRS — NonEquityTransparencyRecord (FULNCR / DLTNCR)',
            'ISIN-level non-equity transparency thresholds — all CFI asset types',
            NonEquityTransparencyRecord.get_schema(),
            show_source=True,
        )

    console.print('[dim]CSV Column shows the original ESMA XML/CSV column name.[/dim]\n')


# ---------------------------------------------------------------------------
# cfi
# ---------------------------------------------------------------------------

@schema_cli.command(name='cfi')
@click.option('--category', default=None, metavar='LETTER',
              help='Show groups for a specific CFI category letter (E, D, C, F, H, I, J, O, S, ...).')
def cfi_schema(category: str | None):
    """Show CFI (ISO 10962) asset category and group taxonomy.

    Without --category, lists all top-level CFI categories.
    Use --category LETTER to see sub-groups for that category.

    \b
    Examples:
      schema cfi
      schema cfi --category E
      schema cfi --category D
    """
    from esma_dm.models.utils.cfi import (
        Category,
        EquityGroup, DebtGroup, CIVGroup,
        EntitlementsGroup, OptionsGroup, FuturesGroup, SwapsGroup,
        NonStandardGroup, SpotGroup, ForwardsGroup, StrategiesGroup,
        FinancingGroup, ReferentialGroup, OthersGroup,
    )

    if category is None:
        table = Table(title='CFI Category Taxonomy (ISO 10962)', box=box.SIMPLE_HEAD,
                      show_header=True, header_style='bold')
        table.add_column('Code', style='bold cyan', width=6)
        table.add_column('Category')
        table.add_column('FIRDS Asset Types')

        firds_asset_map = {
            'E': 'Equities (shares, ETFs, structured participation)',
            'D': 'Debt (bonds, notes, money market)',
            'C': 'Collective investments (funds, ETFs)',
            'R': 'Entitlements / rights',
            'O': 'Options',
            'F': 'Futures',
            'S': 'Swaps',
            'H': 'Non-standard derivatives (warrants, other options)',
            'I': 'Spot instruments / ETCs',
            'J': 'Forwards',
            'K': 'Strategies',
            'L': 'Financing instruments',
            'T': 'Referential instruments',
            'M': 'Others / miscellaneous',
        }
        for cat in Category:
            letter = cat.value
            table.add_row(letter, cat.name.replace('_', ' ').title(), firds_asset_map.get(letter, ''))
        console.print()
        console.print(table)
        console.print('[dim]Use --category LETTER to see sub-groups. Italicised categories have no FIRDS data.[/dim]\n')
        return

    cat_upper = category.upper()
    group_map = {
        'E': ('Equity Groups',                    EquityGroup),
        'D': ('Debt Instrument Groups',           DebtGroup),
        'C': ('Collective Investment Groups',     CIVGroup),
        'R': ('Entitlement Groups',               EntitlementsGroup),
        'O': ('Listed Options Groups',            OptionsGroup),
        'F': ('Futures Groups',                   FuturesGroup),
        'S': ('Swaps Groups',                     SwapsGroup),
        'H': ('Non-Standard Derivatives Groups',  NonStandardGroup),
        'I': ('Spot Instrument Groups',           SpotGroup),
        'J': ('Forwards Groups',                  ForwardsGroup),
        'K': ('Strategies Groups',                StrategiesGroup),
        'L': ('Financing Instrument Groups',      FinancingGroup),
        'T': ('Referential Instrument Groups',    ReferentialGroup),
        'M': ('Others Groups',                    OthersGroup),
    }

    if cat_upper not in group_map:
        console.print(f'[yellow]No group breakdown available for category {cat_upper}.[/yellow]')
        console.print('[dim]Available categories: ' + ', '.join(group_map.keys()) + '[/dim]\n')
        return

    label, enum_cls = group_map[cat_upper]
    table = Table(title=f'CFI Category {cat_upper} — {label}', box=box.SIMPLE_HEAD,
                  show_header=True, header_style='bold')
    table.add_column('Code', style='bold cyan', width=6)
    table.add_column('Group Name')
    table.add_column('Description')
    console.print()
    for member in enum_cls:
        name = member.name.replace('_', ' ').title()
        table.add_row(member.value, name, '')
    console.print(table)
    console.print(f'[dim]CFI code position 2 for category {cat_upper}.[/dim]\n')


# ---------------------------------------------------------------------------
# decode
# ---------------------------------------------------------------------------

@schema_cli.command(name='decode')
@click.argument('cfi_code')
def decode_cfi_command(cfi_code: str):
    """Decode a 6-character CFI code to its category, group, and attributes.

    \b
    Examples:
      schema decode ESVUFR
      schema decode DBFTXX
      schema decode OCESCN
    """
    from esma_dm.models.utils.cfi import decode_cfi, get_attribute_labels

    result = decode_cfi(cfi_code)

    if result is None:
        console.print(f'[red]Could not decode CFI code: {cfi_code!r}. Must be exactly 6 characters with a valid category.[/red]\n')
        raise SystemExit(1)

    labels = get_attribute_labels(cfi_code)

    console.print()
    console.print(f'[bold]CFI Code:[/bold] [bold cyan]{result.code}[/bold cyan]')
    console.print(f'[bold]Category:[/bold] {result.category_code} - {result.category}')
    console.print(f'[bold]Group   :[/bold] {result.group_code} - {result.group}')

    if result.attributes:
        table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style='bold', padding=(0, 1))
        table.add_column('Attribute', style='bold')
        table.add_column('Value')
        for key, value in result.attributes.items():
            label = labels.get(key, key.replace('_', ' ').title())
            table.add_row(label, str(value))
        console.print()
        console.print(table)
    else:
        console.print('[dim]No defined attributes for this group.[/dim]')

    console.print()


# ---------------------------------------------------------------------------
# enums
# ---------------------------------------------------------------------------

ENUM_CHOICES = click.Choice(
    ['methodology', 'classification', 'file-types', 'segmentation', 'all'],
    case_sensitive=False,
)


@schema_cli.command(name='enums')
@click.option('--name', type=ENUM_CHOICES, default='all',
              show_default=True,
              help='Which enum to display.')
def enums_schema(name: str):
    """Show FITRS transparency enum definitions and code descriptions.

    \b
    Available enums:
      methodology     Calculation methodology codes (SINT, YEAR, ESTM, FFWK)
      classification  Equity instrument classification codes (SHRS, DPRS, ETFS, OTHR)
      file-types      FITRS file type codes and their descriptions
      segmentation    Non-equity sub-class segmentation criteria codes
      all             Show all of the above (default)
    """
    from esma_dm.models.transparency_enums import (
        Methodology, InstrumentClassification, FileType, SegmentationCriteria,
    )

    def _render_enum(title: str, subtitle: str, enum_cls) -> None:
        table = Table(title=title, box=box.SIMPLE_HEAD, show_header=True, header_style='bold')
        table.add_column('Code', style='bold cyan')
        table.add_column('Description')
        for member in enum_cls:
            table.add_row(member.name, member.value)
        console.print()
        if subtitle:
            console.print(f'[dim]{subtitle}[/dim]')
        console.print(table)

    target = name.lower()

    if target in ('methodology', 'all'):
        _render_enum(
            'Methodology Codes',
            'Mthdlgy field in FULECR / FULNCR records',
            Methodology,
        )

    if target in ('classification', 'all'):
        _render_enum(
            'Equity Instrument Classification Codes',
            'FinInstrmClssfctn field in FULECR records',
            InstrumentClassification,
        )

    if target in ('file-types', 'all'):
        _render_enum(
            'FITRS File Type Codes',
            'Used in file names and download filters',
            FileType,
        )

    if target in ('segmentation', 'all'):
        _render_enum(
            'Non-Equity Segmentation Criteria Codes',
            'CritNm / CritNm_2 fields in FULNCR records (ESMA65-8-5240 §2.3)',
            SegmentationCriteria,
        )

    console.print()


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@schema_cli.command(name='list')
def list_schemas():
    """List all available schema commands and models."""
    table = Table(title='Available Schema Commands', box=box.SIMPLE_HEAD,
                  show_header=True, header_style='bold')
    table.add_column('Command', style='bold cyan')
    table.add_column('Options')
    table.add_column('Description')

    rows = [
        ('schema firds', '', 'Base Instrument fields (all asset types)'),
        ('schema firds', '--asset equity', 'EquityInstrument fields (CFI E*)'),
        ('schema firds', '--asset debt', 'DebtInstrument fields (CFI D*)'),
        ('schema firds', '--asset derivative', 'DerivativeInstrument fields (CFI F/H/I/J/S*)'),
        ('schema firds', '--asset option', 'OptionAttributes (nested in derivatives)'),
        ('schema firds', '--asset future', 'FutureAttributes (nested in derivatives)'),
        ('schema firds', '--asset trading-venue', 'TradingVenueAttributes (nested)'),
        ('schema firds', '--asset technical', 'TechnicalAttributes (nested)'),
        ('schema transparency', '', 'All transparency schemas'),
        ('schema transparency', '--type equity', 'EquityTransparencyRecord (FULECR)'),
        ('schema transparency', '--type non-equity', 'NonEquityTransparencyRecord (FULNCR)'),
        ('schema cfi', '', 'CFI category taxonomy (all categories)'),
        ('schema cfi', '--category E', 'CFI group breakdown for a category'),
        ('schema decode', 'ESVUFR', 'Decode a 6-character CFI code to its attributes'),
        ('schema enums', '', 'All FITRS enum definitions'),
        ('schema enums', '--name methodology', 'Methodology codes and descriptions'),
        ('schema enums', '--name classification', 'Instrument classification codes'),
        ('schema enums', '--name file-types', 'FITRS file type codes'),
        ('schema enums', '--name segmentation', 'Segmentation criteria codes'),
    ]

    for cmd, opts, desc in rows:
        table.add_row(cmd, f'[dim]{opts}[/dim]' if opts else '', desc)

    console.print()
    console.print(table)
    console.print()
