"""
Per-statement diagnostic for _apply_new on DLTINS_20260118 full file.
Runs all 4 SQL statements individually to find which one raises the FK error.
"""
import traceback
import pandas as pd
import sys
sys.path.insert(0, ".")

from esma_dm.storage.history.store import HistoryStore, _extract_dltins_date, DLTINS_PREFIXES
from datetime import datetime, timedelta
from pathlib import Path

csv_path = Path("downloads/data/firds/DLTINS_20260118_01of01_data.csv")

# Fresh DB for isolation
import duckdb, shutil, os
db_path = "downloads/data/firds/_test_hist.duckdb"
if os.path.exists(db_path):
    os.remove(db_path)

store = HistoryStore(db_path=db_path)
store.initialize()

# Populate instruments + listings from FULINS baseline
fulins1 = "downloads/data/firds/FULINS_E_20260117_01of02_data.csv"
fulins2 = "downloads/data/firds/FULINS_E_20260117_02of02_data.csv"
for f in [fulins1, fulins2]:
    r = store.bulk_load_fulins(Path(f))
    print(f"Loaded {f}: {r['isins_inserted']:,} ISINs, {r['listings_inserted']:,} listings")

print(f"\nDB state after init:")
print(f"  instruments: {store.con.execute('SELECT COUNT(*) FROM instruments').fetchone()[0]:,}")
print(f"  listings:    {store.con.execute('SELECT COUNT(*) FROM listings').fetchone()[0]:,}")
print()

# Load the DLTINS file
print(f"Loading {csv_path.name}...")
df = pd.read_csv(csv_path, low_memory=False)
print(f"Total rows: {len(df):,}")
print(f"Record type counts:")
print(df["_record_type"].value_counts())

pub_date = "2026-01-18"
close_date = "2026-01-17"
now_ts = datetime.now().isoformat()

for record_type in ("NEW", "MODIFIED", "TERMINATED", "CANCELLED"):
    prefix = DLTINS_PREFIXES[record_type]
    subset = df[df["_record_type"] == record_type].copy()
    if subset.empty:
        print(f"\n{record_type}: no records, skipping")
        continue

    normalized = store._normalize_dltins(subset, prefix)
    if normalized.empty:
        print(f"\n{record_type}: normalized empty, skipping")
        continue

    by_isin = normalized.drop_duplicates(subset=["isin"], keep="last")
    print(f"\n{record_type}: {len(normalized):,} rows, {len(by_isin):,} unique ISINs")

    if record_type == "NEW":
        store.con.register("_new_recs", by_isin)
        store.con.register("_new_listings", normalized)
        stmts = [
            ("S1 insert instrument_history", """
                INSERT INTO instrument_history
                    (isin, version_number, valid_from_date, valid_to_date,
                     record_type, cfi_code, full_name, issuer,
                     source_file, source_file_type, indexed_at)
                SELECT i.isin, i.version_number, i.valid_from_date, ?,
                       i.record_type, i.cfi_code, i.full_name, i.issuer,
                       i.source_file, i.source_file_type, i.indexed_at
                FROM instruments i
                INNER JOIN _new_recs r ON i.isin = r.isin
                WHERE i.latest_record_flag = TRUE
                ON CONFLICT (isin, version_number) DO NOTHING
            """, [close_date]),
            ("S2 UPDATE instruments version_number", """
                UPDATE instruments
                SET version_number = instruments.version_number + 1,
                    valid_from_date = ?, valid_to_date = NULL,
                    latest_record_flag = TRUE, record_type = 'NEW',
                    source_file = ?, source_file_type = 'DLTINS',
                    last_update_timestamp = ?
                WHERE isin IN (SELECT isin FROM _new_recs)
                  AND latest_record_flag = TRUE
            """, [pub_date, csv_path.name, now_ts]),
            ("S3 INSERT new instruments", """
                INSERT INTO instruments (
                    isin, cfi_code, instrument_type, full_name, short_name,
                    issuer, currency, competent_authority,
                    valid_from_date, latest_record_flag, record_type,
                    version_number, source_file, source_file_type, indexed_at
                )
                SELECT DISTINCT ON (r.isin)
                    r.isin, r.cfi_code, r.instrument_type, r.full_name, r.short_name,
                    r.issuer, r.currency, r.competent_authority,
                    ?, TRUE, 'NEW', 1, ?, 'DLTINS', ?
                FROM _new_recs r
                WHERE r.isin NOT IN (SELECT isin FROM instruments)
                ON CONFLICT (isin) DO NOTHING
            """, [pub_date, csv_path.name, now_ts]),
            ("S4 INSERT listings", """
                INSERT INTO listings (
                    isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, source_file, indexed_at
                )
                SELECT isin, trading_venue_id, first_trade_date, termination_date,
                    admission_approval_date, request_for_admission_date,
                    issuer_request, ?, ?
                FROM _new_listings
                WHERE isin IN (SELECT isin FROM instruments)
                ON CONFLICT (isin, trading_venue_id) DO NOTHING
            """, [csv_path.name, now_ts]),
        ]
        for label, sql, params in stmts:
            try:
                store.con.execute(sql, params)
                print(f"  {label}: OK")
            except Exception as e:
                print(f"  {label}: FAILED")
                traceback.print_exc()
                break
        store.con.unregister("_new_recs")
        store.con.unregister("_new_listings")
        break  # only test NEW for now
