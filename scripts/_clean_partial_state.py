"""Clean partial state left by an interrupted hist update run."""
import duckdb

con = duckdb.connect("esma_dm/storage/duckdb/database/esma_hist.duckdb")

before_hist = con.execute("SELECT COUNT(*) FROM instrument_history").fetchone()[0]
before_canc = con.execute("SELECT COUNT(*) FROM cancellations").fetchone()[0]

# Remove rows that were applied without being logged in processing_log.
# For instrument_history: rows where the source_file is NOT in processing_log.
con.execute("""
    DELETE FROM instrument_history
    WHERE source_file NOT IN (SELECT file_name FROM processing_log)
       OR source_file IS NULL
""")
con.execute("""
    DELETE FROM cancellations
    WHERE source_file NOT IN (SELECT file_name FROM processing_log)
       OR source_file IS NULL
""")

after_hist = con.execute("SELECT COUNT(*) FROM instrument_history").fetchone()[0]
after_canc = con.execute("SELECT COUNT(*) FROM cancellations").fetchone()[0]

print(f"instrument_history: {before_hist} -> {after_hist} ({before_hist - after_hist} removed)")
print(f"cancellations:      {before_canc} -> {after_canc} ({before_canc - after_canc} removed)")
print("Done.")
