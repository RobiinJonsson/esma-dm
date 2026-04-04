"""Isolate which SQL statement causes the FK constraint error in _apply_new."""
import duckdb
import pandas as pd
import sys
sys.path.insert(0, ".")

from esma_dm.storage.history.store import HistoryStore, _extract_dltins_date, DLTINS_PREFIXES
from datetime import datetime, timedelta
from pathlib import Path

csv_path = Path("downloads/data/firds/DLTINS_20260118_01of01_data.csv")

store = HistoryStore(db_path="esma_dm/storage/duckdb/database/esma_hist.duckdb")
pub_date = _extract_dltins_date(csv_path.name)
close_date = (datetime.strptime(pub_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

df = pd.read_csv(csv_path, low_memory=False)
print(f"Total rows: {len(df)}")
print(f"Record type counts:\n{df['_record_type'].value_counts()}")

# Process NEW records only
subset = df[df["_record_type"] == "NEW"].copy()
print(f"\nNEW records: {len(subset)}")

normalized = store._normalize_dltins(subset, "NewRcrd")
print(f"Normalized (per-listing): {len(normalized)}")

by_isin = normalized.drop_duplicates(subset=["isin"], keep="last")
print(f"Deduplicated by ISIN: {len(by_isin)}")

# Check the failing ISIN
target_isin = "NO0013711739"
print(f"\nTarget ISIN in normalized: {target_isin in normalized['isin'].values}")
print(f"Target ISIN in by_isin:    {target_isin in by_isin['isin'].values}")

# Check instruments and listings for the target ISIN
print(f"\nIn instruments: {store.con.execute('SELECT COUNT(*) FROM instruments WHERE isin = ?', [target_isin]).fetchone()[0]}")
print(f"In listings:    {store.con.execute('SELECT COUNT(*) FROM listings WHERE isin = ?', [target_isin]).fetchone()[0]}")

now_ts = datetime.now().isoformat()
source_file = csv_path.name

store.con.register("_new_recs", by_isin)
store.con.register("_new_listings", normalized)

print("\n--- Running SQL statements individually ---")

sqls = [
    ("1 Archive to instrument_history", """
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
    ("2 UPDATE instruments version_number", """
        UPDATE instruments
        SET
            version_number = instruments.version_number + 1,
            valid_from_date = ?,
            valid_to_date = NULL,
            latest_record_flag = TRUE,
            record_type = 'NEW',
            source_file = ?,
            source_file_type = 'DLTINS',
            last_update_timestamp = ?
        WHERE isin IN (SELECT isin FROM _new_recs)
          AND latest_record_flag = TRUE
    """, [pub_date, source_file, now_ts]),
    ("3 INSERT truly new ISINs into instruments", """
        INSERT INTO instruments (
            isin, cfi_code, instrument_type, full_name, short_name,
            issuer, currency, competent_authority,
            valid_from_date, latest_record_flag,
            record_type, version_number,
            source_file, source_file_type, indexed_at
        )
        SELECT DISTINCT ON (r.isin)
            r.isin, r.cfi_code, r.instrument_type, r.full_name, r.short_name,
            r.issuer, r.currency, r.competent_authority,
            ?, TRUE, 'NEW', 1, ?, 'DLTINS', ?
        FROM _new_recs r
        WHERE r.isin NOT IN (SELECT isin FROM instruments)
        ON CONFLICT (isin) DO NOTHING
    """, [pub_date, source_file, now_ts]),
    ("4 INSERT listings", """
        INSERT INTO listings (
            isin, trading_venue_id, first_trade_date, termination_date,
            admission_approval_date, request_for_admission_date,
            issuer_request, source_file, indexed_at
        )
        SELECT
            isin, trading_venue_id, first_trade_date, termination_date,
            admission_approval_date, request_for_admission_date,
            issuer_request, ?, ?
        FROM _new_listings
        WHERE isin IN (SELECT isin FROM instruments)
        ON CONFLICT (isin, trading_venue_id) DO NOTHING
    """, [source_file, now_ts]),
]

for label, sql, params in sqls:
    try:
        store.con.execute(sql, params)
        print(f"  SQL {label}: OK")
    except Exception as e:
        print(f"  SQL {label}: FAILED -> {e}")
        break

store.con.unregister("_new_recs")
store.con.unregister("_new_listings")
