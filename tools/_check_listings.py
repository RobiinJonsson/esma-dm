"""Verify whether listings rows are genuine multi-venue or duplicated."""
import duckdb

con = duckdb.connect(r'esma_dm\storage\duckdb\database\esma_current.duckdb', read_only=True)

total      = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
distinct_isin = con.execute("SELECT COUNT(DISTINCT isin) FROM listings").fetchone()[0]
distinct_pairs = con.execute(
    "SELECT COUNT(*) FROM (SELECT DISTINCT isin, trading_venue_id FROM listings)"
).fetchone()[0]
instruments = con.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]

print(f"instruments table        : {instruments:>12,}")
print(f"listings total rows      : {total:>12,}  ({total/instruments:.1f}x instruments)")
print(f"listings distinct ISINs  : {distinct_isin:>12,}")
print(f"listings distinct (isin, venue): {distinct_pairs:>9,}  ({distinct_pairs/instruments:.1f}x instruments)")
print(f"duplicate rows (total - distinct pairs): {total - distinct_pairs:>9,}")
print()

# Distribution of rows per isin
print("Listings-per-ISIN distribution:")
rows = con.execute("""
    SELECT cnt, COUNT(*) as isins
    FROM (SELECT isin, COUNT(*) as cnt FROM listings GROUP BY isin)
    GROUP BY cnt ORDER BY cnt
    LIMIT 20
""").fetchall()
for cnt, isins in rows:
    print(f"  {cnt} listings: {isins:,} ISINs")

print()

# Are there exact duplicates (same isin + venue combination appearing more than once)?
dupe_venue_pairs = con.execute("""
    SELECT COUNT(*) FROM (
        SELECT isin, trading_venue_id, COUNT(*) as n
        FROM listings
        GROUP BY isin, trading_venue_id
        HAVING n > 1
    )
""").fetchone()[0]
print(f"(isin, venue) pairs with duplicates: {dupe_venue_pairs:,}")

# Sample a few duplicated pairs
if dupe_venue_pairs > 0:
    print("\nSample duplicated (isin, venue) pairs:")
    samples = con.execute("""
        SELECT isin, trading_venue_id, COUNT(*) as n
        FROM listings
        GROUP BY isin, trading_venue_id
        HAVING n > 1
        ORDER BY n DESC
        LIMIT 5
    """).fetchall()
    for isin, venue, n in samples:
        print(f"  {isin}  {venue}  x{n}")

con.close()
