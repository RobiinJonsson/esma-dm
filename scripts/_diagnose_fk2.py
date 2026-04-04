"""Diagnose FK constraint issues in the history database."""
import duckdb

con = duckdb.connect("esma_dm/storage/duckdb/database/esma_hist.duckdb", read_only=True)

print("=== Orphan checks (child rows with no parent in instruments) ===")
for tbl in ["listings", "instrument_history"]:
    n = con.execute(
        f"SELECT COUNT(*) FROM {tbl} WHERE isin NOT IN (SELECT isin FROM instruments)"
    ).fetchone()[0]
    print(f"  {tbl} orphans: {n:,}")

print()

# Test DuckDB FK behavior on UPDATE non-key column
print("=== Testing DuckDB FK behavior ===")
mem = duckdb.connect(":memory:")
mem.execute("CREATE TABLE parent (id VARCHAR PRIMARY KEY, ver INT)")
mem.execute("CREATE TABLE child (pk INT PRIMARY KEY, pid VARCHAR, FOREIGN KEY (pid) REFERENCES parent(id))")
mem.execute("INSERT INTO parent VALUES ('A', 1)")
mem.execute("INSERT INTO child VALUES (1, 'A')")

# Test 1: UPDATE non-key column on parent (should succeed)
try:
    mem.execute("UPDATE parent SET ver = 2 WHERE id = 'A'")
    print("  UPDATE parent non-key with child referencing it: OK")
except Exception as e:
    print(f"  UPDATE parent non-key with child referencing it: FAIL -> {e}")

# Test 2: UPDATE key column on parent (should fail)
try:
    mem.execute("UPDATE parent SET id = 'B' WHERE id = 'A'")
    print("  UPDATE parent key with child referencing it: OK (unexpected)")
except Exception as e:
    print(f"  UPDATE parent key with child referencing it: FAIL (expected) -> {e}")

print()
print("=== instrument_history sample (first 10 rows) ===")
print(con.execute("SELECT isin, version_number, valid_from_date, record_type FROM instrument_history LIMIT 10").fetchdf())
