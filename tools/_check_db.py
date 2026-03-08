"""Quick diagnostic for data quality after re-index."""
import duckdb
import os

path = r'esma_dm\storage\duckdb\database\esma_current.duckdb'
size_mb = os.path.getsize(path) / (1024 * 1024)
print(f'DB size: {size_mb:.0f} MB')

con = duckdb.connect(path, read_only=True)

# Listings duplication
listings = con.execute('SELECT COUNT(*) FROM listings').fetchone()[0]
listings_distinct = con.execute('SELECT COUNT(DISTINCT isin) FROM listings').fetchone()[0]
print(f'listings: {listings:,} rows  ({listings_distinct:,} distinct ISINs)')

# H in master vs detail
h_master = con.execute("SELECT COUNT(*) FROM instruments WHERE instrument_type='H'").fetchone()[0]
h_detail = con.execute('SELECT COUNT(*) FROM non_standard_instruments').fetchone()[0]
print(f'H master: {h_master:,}  non_standard_instruments: {h_detail:,}')

# All detail table counts
for tbl in ['equity_instruments', 'debt_instruments', 'futures_instruments',
            'option_instruments', 'swap_instruments', 'forward_instruments',
            'rights_instruments', 'civ_instruments', 'spot_instruments',
            'non_standard_instruments', 'strategy_instruments',
            'financing_instruments', 'other_instruments', 'referential_instruments']:
    n = con.execute(f'SELECT COUNT(*) FROM {tbl}').fetchone()[0]
    print(f'  {tbl}: {n:,}')

con.close()
