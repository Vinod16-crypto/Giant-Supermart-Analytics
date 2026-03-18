"""
=============================================================
STEP 1: DATA INGESTION
File: ingestion/ingest.py

PURPOSE:
  Load raw CSV data into a SQLite database (local dev).
  In production, swap the connection string for PostgreSQL.

WHAT THIS SCRIPT DOES:
  1. Reads the raw CSV file using pandas
  2. Validates that the file loaded correctly
  3. Creates a SQLite database (our "data warehouse" locally)
  4. Loads data into a raw_transactions table
  5. Prints a summary report

HOW TO RUN:
  python ingestion/ingest.py

BEGINNER TIP:
  SQLite is a file-based database — no installation needed!
  Think of it like a very powerful Excel file.
  PostgreSQL is what companies use in production.
=============================================================
"""

import pandas as pd
import sqlite3
import os
import sys

# ── Configuration ─────────────────────────────────────────
# Change these paths to match your setup
CSV_PATH = "data/giant_supermart_120k.csv"          # Raw data file
DB_PATH  = "data/giant_supermart.db"                 # SQLite database file
TABLE_NAME = "raw_transactions"                      # Table to load into

# ── STEP 1A: Load CSV into pandas DataFrame ───────────────
print("=" * 60)
print("  GIANT SUPERMART — DATA INGESTION PIPELINE")
print("=" * 60)
print(f"\n[1/5] Loading raw CSV from: {CSV_PATH}")

if not os.path.exists(CSV_PATH):
    print(f"  ERROR: File not found at {CSV_PATH}")
    print("  Please make sure the CSV file is in the 'data/' folder")
    sys.exit(1)

df = pd.read_csv(CSV_PATH, parse_dates=["date"])
print(f"  ✅ Loaded {len(df):,} rows and {len(df.columns)} columns")
print(f"  Date range: {df['date'].min().date()} → {df['date'].max().date()}")


# ── STEP 1B: Basic validation before loading ─────────────
print("\n[2/5] Running pre-load validation checks...")

issues = []
# Check for missing required columns
required_cols = [
    "transaction_id", "date", "region", "store_name", "category",
    "product_name", "quantity", "total_sgd", "payment_method"
]
for col in required_cols:
    if col not in df.columns:
        issues.append(f"Missing required column: {col}")

# Check for duplicate transaction IDs
dupe_count = df["transaction_id"].duplicated().sum()
if dupe_count > 0:
    issues.append(f"Found {dupe_count} duplicate transaction_ids")

# Check for negative prices
neg_prices = (df["total_sgd"] < 0).sum()
if neg_prices > 0:
    issues.append(f"Found {neg_prices} negative total_sgd values")

if issues:
    print("  ⚠️  Validation warnings:")
    for issue in issues:
        print(f"     - {issue}")
else:
    print("  ✅ All validation checks passed!")


# ── STEP 1C: Connect to SQLite database ──────────────────
print(f"\n[3/5] Connecting to database: {DB_PATH}")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = sqlite3.connect(DB_PATH)
print("  ✅ Database connection established")


# ── STEP 1D: Write data to the database ──────────────────
print(f"\n[4/5] Loading data into table: '{TABLE_NAME}'")

# if_exists="replace" → drops and recreates the table each run
# Use "append" if you want to add rows to existing table
df.to_sql(
    name=TABLE_NAME,
    con=conn,
    if_exists="replace",   # "replace" for fresh load, "append" for incremental
    index=False,           # Don't write pandas row numbers as a column
    chunksize=5000         # Write 5000 rows at a time (memory efficient)
)
print(f"  ✅ Loaded {len(df):,} rows into '{TABLE_NAME}'")


# ── STEP 1E: Verify the load was successful ───────────────
print("\n[5/5] Verifying data load...")

# Count rows in database
cursor = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
db_count = cursor.fetchone()[0]
print(f"  Rows in CSV:      {len(df):,}")
print(f"  Rows in database: {db_count:,}")

if db_count == len(df):
    print("  ✅ Row counts match — ingestion successful!")
else:
    print("  ❌ Row count mismatch — please investigate!")

# Show a sample of what was loaded
print("\n  Sample data (first 3 rows):")
sample = pd.read_sql("SELECT transaction_id, date, region, category, total_sgd FROM raw_transactions LIMIT 3", conn)
print(sample.to_string(index=False))

# ── Summary Report ────────────────────────────────────────
print("\n" + "=" * 60)
print("  INGESTION SUMMARY REPORT")
print("=" * 60)
print(f"  Source file  : {CSV_PATH}")
print(f"  Database     : {DB_PATH}")
print(f"  Table        : {TABLE_NAME}")
print(f"  Total rows   : {db_count:,}")
print(f"  Columns      : {len(df.columns)}")
print(f"  Date range   : {df['date'].min().date()} to {df['date'].max().date()}")
print(f"  Regions      : {sorted(df['region'].unique())}")
print(f"  Total Revenue: SGD {df['total_sgd'].sum():,.2f}")
print("=" * 60)
print("\n  ✅ INGESTION COMPLETE! Run warehouse/create_star_schema.py next.")
print()

conn.close()
