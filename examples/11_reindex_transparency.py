"""
Example: Sync Cache and Re-Index Transparency Data

Steps:
  1. Query ESMA for the full file list and find the latest publication date.
  2. Download any missing files for that date (skips files already cached).
  3. Delete cached CSV files from older dates to keep the cache clean.
  4. Clear transparency / subclass_transparency tables in DuckDB.
  5. Re-index all FULECR + FULNCR files from the clean cache.
  6. Download and index FULNCR_NYAR sub-class data.
  7. Print a verification summary.
"""

import pathlib
import re
import logging
import esma_dm as edm
from esma_dm import FITRSClient
from esma_dm.utils import Utils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('reindex')

CACHE_DIR = pathlib.Path('downloads/data/fitrs')
# Matches: FULECR_20260307_E_1of1_data.csv
_STEM_RE = re.compile(r'^(FULECR|FULNCR|DLTECR|DLTNCR)_(\d{8})_')


def _count(con, table, where=''):
    sql = f"SELECT COUNT(*) FROM {table}"
    if where:
        sql += f" WHERE {where}"
    return con.execute(sql).fetchone()[0]


def sync_cache(fitrs):
    """
    Ensure the cache holds a complete set of FULECR/FULNCR files for the
    latest available date. Missing files are downloaded; old-date files are removed.
    Returns the latest date string (e.g. '20260307').
    """
    print("\n1. Fetching ESMA File List")
    print("-" * 60)
    files_df = fitrs.get_file_list()

    # ISIN-level full files only (exclude NYAR/SISC sub-class variants)
    mask = (
        files_df['file_name'].str.match(r'^(FULECR|FULNCR)_', na=False) &
        ~files_df['file_name'].str.contains('NYAR|SISC', na=False)
    )
    isin_files = files_df[mask].copy()
    isin_files['date'] = isin_files['file_name'].str.extract(r'_((\d{8}))_')[0]

    latest_date = isin_files['date'].max()
    latest = isin_files[isin_files['date'] == latest_date]

    print(f"  Latest date     : {latest_date}")
    print(f"  Files available : {len(latest)}")

    # Classify: cached vs missing
    already_cached, to_download = [], []
    for _, row in latest.iterrows():
        stem = Utils.extract_file_name_from_url(row['download_link'])
        cache_file = CACHE_DIR / f"{stem}_data.csv"
        if cache_file.exists() and cache_file.stat().st_size > 0:
            already_cached.append(row['file_name'])
        else:
            to_download.append(row)

    print(f"  Already cached  : {len(already_cached)}")
    print(f"  To download     : {len(to_download)}")

    # Download missing files
    if to_download:
        print(f"\n  Downloading {len(to_download)} missing file(s)...")
        for i, row in enumerate(to_download, 1):
            print(f"    [{i}/{len(to_download)}] {row['file_name']}")
            try:
                fitrs.download_file(row['download_link'], update=False)
            except Exception as e:
                logger.error(f"Failed to download {row['file_name']}: {e}")

    # Remove old-date cached CSVs
    print("\n  Cleaning old-date cache files...")
    removed = 0
    for path in sorted(CACHE_DIR.glob('*_data.csv')):
        m = _STEM_RE.match(path.name)
        if not m:
            continue
        if 'NYAR' in path.name or 'SISC' in path.name:
            continue
        file_date = m.group(2)
        if file_date < latest_date:
            logger.info(f"Removing old file: {path.name}")
            path.unlink()
            removed += 1
    print(f"  Removed {removed} old-date file(s)")

    return latest_date


def clear_tables(con):
    print("\n2. Clearing Transparency Tables")
    print("-" * 60)
    # Child tables must be cleared before parent (foreign key constraints)
    for table in ('equity_transparency', 'non_equity_transparency',
                  'transparency', 'subclass_transparency'):
        count = _count(con, table)
        con.execute(f"DELETE FROM {table}")
        if count:
            print(f"  Deleted {count:,} rows from {table}")


def reindex_isin_files(fitrs):
    print("\n3. Re-Indexing FULECR + FULNCR from Cache")
    print("-" * 60)
    csv_files = [
        f for f in CACHE_DIR.glob('FUL[EN]CR_[0-9]*_data.csv')
        if 'NYAR' not in f.name and 'SISC' not in f.name
    ]
    print(f"  CSV files in cache : {len(csv_files)}")

    result = fitrs.index_cached_files(file_types=['FULECR', 'FULNCR'])
    print(f"  Files processed    : {result.get('files_processed', 0)}")
    print(f"  Files skipped      : {result.get('files_skipped', 0)}")
    print(f"  Records loaded     : {result.get('total_records', 0):,}")

    errors = [d for d in result.get('details', []) if d.get('status') == 'error']
    if errors:
        print(f"\n  Errors ({len(errors)}):")
        for e in errors[:5]:
            print(f"    {e['file']}: {e.get('error', '')[:100]}")


def reindex_subclass(fitrs):
    print("\n4. Indexing Sub-Class Data (FULNCR_NYAR)")
    print("-" * 60)
    result = edm.transparency.index('FULNCR_NYAR', latest_only=True)
    print(f"  Status          : {result.get('status')}")
    print(f"  Files processed : {result.get('files_processed', 0)}")
    print(f"  Records loaded  : {result.get('total_records', 0):,}")


def print_summary(con):
    print("\n5. Verification Summary")
    print("-" * 60)
    total  = _count(con, 'transparency')
    liquid = _count(con, 'transparency', 'liquid_market IS NOT NULL')
    eq     = _count(con, 'transparency', "instrument_type = 'equity'")
    neq    = _count(con, 'transparency', "instrument_type = 'non_equity'")
    sub    = _count(con, 'subclass_transparency')

    print(f"  transparency total           : {total:,}")
    print(f"  - equity                     : {eq:,}")
    print(f"  - non-equity                 : {neq:,}")
    print(f"  rows with liquid_market set  : {liquid:,}")
    print(f"  subclass_transparency total  : {sub:,}")

    liq_df = con.execute("""
        SELECT instrument_type,
               COUNT(*) AS total,
               SUM(CASE WHEN liquid_market THEN 1 ELSE 0 END) AS liquid,
               ROUND(100.0 * SUM(CASE WHEN liquid_market THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0), 1) AS pct_liquid
        FROM transparency
        WHERE liquid_market IS NOT NULL
        GROUP BY instrument_type
        ORDER BY instrument_type
    """).fetchdf()
    if not liq_df.empty:
        print("\n  Liquidity by instrument type:")
        print(liq_df.to_string(index=False))

    if sub > 0:
        sub_df = con.execute("""
            SELECT asset_class, COUNT(*) AS records
            FROM subclass_transparency
            GROUP BY asset_class
            ORDER BY records DESC
            LIMIT 10
        """).fetchdf()
        print("\n  Sub-class asset classes (top 10):")
        print(sub_df.to_string(index=False))


def main():
    print("ESMA Data Manager - Sync Cache and Re-Index Transparency")
    print("=" * 60)

    fitrs = FITRSClient()
    con = fitrs.data_store.con

    sync_cache(fitrs)
    clear_tables(con)
    reindex_isin_files(fitrs)
    reindex_subclass(fitrs)
    print_summary(con)


if __name__ == "__main__":
    main()
