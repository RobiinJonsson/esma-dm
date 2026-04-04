import duckdb

con = duckdb.connect("esma_dm/storage/duckdb/database/esma_hist.duckdb", read_only=True)

isin = "CA02315E1051"
print(f"instruments {isin}:")
print(con.execute(
    "SELECT isin, version_number, latest_record_flag, record_type FROM instruments WHERE isin = ?",
    [isin]
).fetchdf())

print(f"\nlistings {isin}:")
print(con.execute("SELECT isin, trading_venue_id FROM listings WHERE isin = ?", [isin]).fetchdf())

print(f"\ninstrument_history {isin}:")
print(con.execute("SELECT isin, version_number FROM instrument_history WHERE isin = ?", [isin]).fetchdf())

print("\nprocessing_log (applied delta files):")
print(con.execute(
    "SELECT file_name, records_new, records_modified, records_terminated, records_cancelled, records_error FROM processing_log ORDER BY file_date"
).fetchdf())

print("\nTable counts:")
for t in ["instruments", "listings", "instrument_history", "cancellations"]:
    n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"  {t}: {n:,}")
