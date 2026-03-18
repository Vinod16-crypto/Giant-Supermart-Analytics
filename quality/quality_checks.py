"""
=============================================================
STEP 4: DATA QUALITY TESTING
File: quality/quality_checks.py

PURPOSE:
  Data quality = making sure your data is TRUSTWORTHY.
  Bad data = bad insights = bad business decisions!

TYPES OF TESTS WE RUN:
  1. NULL checks       → Are required fields empty?
  2. Duplicate checks  → Are there repeat records?
  3. Range checks      → Are numbers within valid ranges?
  4. Referential integrity → Do all FK relations exist?
  5. Business logic    → Do totals make mathematical sense?
  6. Distribution checks → Is the data realistic?

BEGINNER TIP:
  Think of this like a doctor checking your health report
  before signing off — we validate everything before
  the business uses the data.

HOW TO RUN:
  python quality/quality_checks.py
=============================================================
"""

import pandas as pd
import sqlite3
import json
from datetime import datetime

DB_PATH = "data/giant_supermart.db"

print("=" * 60)
print("  GIANT SUPERMART — DATA QUALITY CHECKS")
print("=" * 60)

conn = sqlite3.connect(DB_PATH)

# Track all test results
test_results = []
PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"

def run_test(test_name, passed, detail, severity="HIGH"):
    status = PASS if passed else (WARN if severity == "LOW" else FAIL)
    test_results.append({
        "test_name": test_name,
        "status": "PASS" if passed else "FAIL",
        "detail": detail,
        "severity": severity
    })
    print(f"  {status}  [{severity}] {test_name}")
    print(f"          {detail}")


# ─────────────────────────────────────────────────────────
# CATEGORY 1: NULL / MISSING VALUE CHECKS
# ─────────────────────────────────────────────────────────
print("\n─── Category 1: NULL Value Checks ───────────────────")

df_fact = pd.read_sql("SELECT * FROM FactSales", conn)

critical_columns = [
    "transaction_id", "date_key", "store_key", "product_key",
    "customer_key", "total_sgd", "quantity", "payment_method"
]
for col in critical_columns:
    null_count = df_fact[col].isna().sum()
    run_test(
        f"NULL check: FactSales.{col}",
        null_count == 0,
        f"Found {null_count} NULL values ({null_count/len(df_fact)*100:.2f}%)"
    )


# ─────────────────────────────────────────────────────────
# CATEGORY 2: DUPLICATE RECORD CHECKS
# ─────────────────────────────────────────────────────────
print("\n─── Category 2: Duplicate Checks ────────────────────")

dupe_txn = df_fact["transaction_id"].duplicated().sum()
run_test(
    "Duplicate transaction_ids",
    dupe_txn == 0,
    f"Found {dupe_txn} duplicate transaction IDs"
)

df_dim_store = pd.read_sql("SELECT * FROM DimStore", conn)
dupe_stores = df_dim_store[["store_name", "region"]].duplicated().sum()
run_test(
    "Duplicate DimStore rows",
    dupe_stores == 0,
    f"Found {dupe_stores} duplicate store records"
)

df_dim_product = pd.read_sql("SELECT * FROM DimProduct", conn)
dupe_products = df_dim_product["product_name"].duplicated().sum()
run_test(
    "Duplicate DimProduct rows",
    dupe_products == 0,
    f"Found {dupe_products} duplicate product records"
)


# ─────────────────────────────────────────────────────────
# CATEGORY 3: RANGE / BOUNDS CHECKS
# ─────────────────────────────────────────────────────────
print("\n─── Category 3: Range / Value Checks ────────────────")

# Prices must be positive
neg_prices = (df_fact["total_sgd"] <= 0).sum()
run_test(
    "Positive total_sgd values",
    neg_prices == 0,
    f"Found {neg_prices} non-positive transaction values"
)

# Quantities must be >= 1
bad_qty = (df_fact["quantity"] < 1).sum()
run_test(
    "Quantity >= 1",
    bad_qty == 0,
    f"Found {bad_qty} zero or negative quantity values"
)

# Discount should be 0 to 1 (0% to 100%)
bad_disc = ((df_fact["discount_pct"] < 0) | (df_fact["discount_pct"] > 1)).sum()
run_test(
    "Discount in range [0, 1]",
    bad_disc == 0,
    f"Found {bad_disc} out-of-range discount values"
)

# GST rate should be 0.07, 0.08, or 0.09
valid_gst = {0.07, 0.08, 0.09}
invalid_gst = (~df_fact["gst_rate"].isin(valid_gst)).sum()
run_test(
    "GST rate in {0.07, 0.08, 0.09}",
    invalid_gst == 0,
    f"Found {invalid_gst} invalid GST rate values"
)

# Date range: 2022-01-01 to 2024-12-31
df_date = pd.read_sql("SELECT * FROM DimDate", conn)
min_year = df_date["year"].min()
max_year = df_date["year"].max()
run_test(
    "Date range 2022–2024",
    min_year >= 2022 and max_year <= 2024,
    f"Date range: {min_year} to {max_year}"
)


# ─────────────────────────────────────────────────────────
# CATEGORY 4: REFERENTIAL INTEGRITY
# Checks that every foreign key points to a real record
# ─────────────────────────────────────────────────────────
print("\n─── Category 4: Referential Integrity ───────────────")

# All store_keys in FactSales must exist in DimStore
orphan_stores = pd.read_sql("""
    SELECT COUNT(*) AS n FROM FactSales f
    LEFT JOIN DimStore s ON f.store_key = s.store_key
    WHERE s.store_key IS NULL
""", conn).iloc[0, 0]
run_test(
    "FactSales → DimStore referential integrity",
    orphan_stores == 0,
    f"Found {orphan_stores} orphaned store_key references"
)

# All product_keys must exist
orphan_products = pd.read_sql("""
    SELECT COUNT(*) AS n FROM FactSales f
    LEFT JOIN DimProduct p ON f.product_key = p.product_key
    WHERE p.product_key IS NULL
""", conn).iloc[0, 0]
run_test(
    "FactSales → DimProduct referential integrity",
    orphan_products == 0,
    f"Found {orphan_products} orphaned product_key references"
)

# All date_keys must exist
orphan_dates = pd.read_sql("""
    SELECT COUNT(*) AS n FROM FactSales f
    LEFT JOIN DimDate d ON f.date_key = d.date_key
    WHERE d.date_key IS NULL
""", conn).iloc[0, 0]
run_test(
    "FactSales → DimDate referential integrity",
    orphan_dates == 0,
    f"Found {orphan_dates} orphaned date_key references"
)


# ─────────────────────────────────────────────────────────
# CATEGORY 5: BUSINESS LOGIC CHECKS
# ─────────────────────────────────────────────────────────
print("\n─── Category 5: Business Logic Checks ───────────────")

# total_sgd should equal subtotal + gst_amount (within rounding tolerance)
df_fact["calculated_total"] = (df_fact["subtotal_sgd"] + df_fact["gst_amount_sgd"]).round(2)
df_fact["total_mismatch"] = abs(df_fact["total_sgd"] - df_fact["calculated_total"]) > 0.02
mismatch_count = df_fact["total_mismatch"].sum()
run_test(
    "total_sgd = subtotal + gst_amount",
    mismatch_count == 0,
    f"Found {mismatch_count} rows where totals don't reconcile (tolerance: 0.02)"
)

# Promotions should have a promo type
promo_no_type = df_fact[(df_fact["is_promotion"]==1) & 
                         (df_fact["promo_type"]=="No Promotion")].shape[0]
run_test(
    "Promo flag matches promo_type",
    promo_no_type == 0,
    f"Found {promo_no_type} promo transactions with 'No Promotion' type",
    severity="LOW"
)

# unit_price_after_disc should be <= unit_price_sgd
bad_disc_price = (df_fact["unit_price_after_disc"] > df_fact["unit_price_sgd"] + 0.01).sum()
run_test(
    "Discounted price <= original price",
    bad_disc_price == 0,
    f"Found {bad_disc_price} rows where discounted price > original"
)

# Gross profit shouldn't be negative
neg_gp = (df_fact["gross_profit_sgd"] < 0).sum()
run_test(
    "No negative gross profit",
    neg_gp == 0,
    f"Found {neg_gp} negative gross profit rows",
    severity="LOW"
)


# ─────────────────────────────────────────────────────────
# CATEGORY 6: DISTRIBUTION / COMPLETENESS CHECKS
# ─────────────────────────────────────────────────────────
print("\n─── Category 6: Distribution Checks ─────────────────")

# All 5 regions present
regions = pd.read_sql("SELECT DISTINCT region FROM DimStore", conn)
run_test(
    "All 5 Singapore regions present",
    len(regions) == 5,
    f"Found {len(regions)} regions: {regions['region'].tolist()}"
)

# Coverage across all 3 years
years = pd.read_sql("SELECT DISTINCT year FROM DimDate", conn)
run_test(
    "All 3 years (2022–2024) present",
    len(years) == 3,
    f"Found years: {sorted(years['year'].tolist())}"
)

# Minimum transactions per month (seasonality sanity check)
monthly = pd.read_sql("""
    SELECT d.year, d.month, COUNT(*) AS cnt
    FROM FactSales f
    JOIN DimDate d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
""", conn)
min_monthly = monthly["cnt"].min()
run_test(
    "Minimum 500 transactions per month",
    min_monthly >= 500,
    f"Minimum monthly transactions: {min_monthly}",
    severity="LOW"
)


# ── Final Report ──────────────────────────────────────────
passed = sum(1 for t in test_results if t["status"] == "PASS")
failed = sum(1 for t in test_results if t["status"] == "FAIL")
total  = len(test_results)
score  = passed / total * 100

print("\n" + "=" * 60)
print("  DATA QUALITY REPORT")
print("=" * 60)
print(f"  Total Tests : {total}")
print(f"  Passed      : {passed}  ✅")
print(f"  Failed      : {failed}  ❌")
print(f"  Quality Score: {score:.1f}%")
print()

if score == 100:
    print("  🌟 PERFECT SCORE — Data is clean and ready for analysis!")
elif score >= 90:
    print("  ✅ GOOD — Minor issues. Review warnings before production.")
elif score >= 75:
    print("  ⚠️  ACCEPTABLE — Some issues found. Review before proceeding.")
else:
    print("  ❌ POOR — Significant data quality issues. Investigate before use.")

# Save report to file
report = {
    "run_timestamp": datetime.now().isoformat(),
    "quality_score": score,
    "total_tests": total,
    "passed": passed,
    "failed": failed,
    "test_details": test_results
}
import json
os.makedirs("docs", exist_ok=True) if False else None
with open("quality/quality_report.json", "w") as f:
    json.dump(report, f, indent=2)

print(f"\n  Report saved to: quality/quality_report.json")
print("\n  ✅ QUALITY CHECKS COMPLETE! Run analysis/eda.py next.")
conn.close()

import os
