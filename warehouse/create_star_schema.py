"""
=============================================================
STEP 2: DATA WAREHOUSE — STAR SCHEMA DESIGN
File: warehouse/create_star_schema.py

PURPOSE:
  Transform raw "flat" data into a Star Schema.
  
WHAT IS A STAR SCHEMA?
  Imagine a star with a big table in the middle (Fact table)
  and smaller tables around it (Dimension tables).
  
  - FACT table    = What happened (sales transactions)
  - DIMENSION tables = Context (Who, What, Where, When, How)

  This makes queries MUCH faster and easier to understand.

OUR STAR SCHEMA:
                  DimDate
                     |
  DimStore ——— FactSales ——— DimProduct
                     |
              DimCustomer — DimPromotion

WHY STAR SCHEMA?
  - Fast queries (fewer JOINs vs normalized tables)
  - Easy for business users to understand
  - Perfect for analytical workloads (OLAP)
  - Industry standard for data warehouses

HOW TO RUN:
  python warehouse/create_star_schema.py
=============================================================
"""

import pandas as pd
import sqlite3
import os

# ── Configuration ─────────────────────────────────────────
DB_PATH = "data/giant_supermart.db"

print("=" * 60)
print("  GIANT SUPERMART — STAR SCHEMA BUILDER")
print("=" * 60)

conn = sqlite3.connect(DB_PATH)

# Load raw data
print("\n[1/7] Loading raw transactions...")
df = pd.read_sql("SELECT * FROM raw_transactions", conn, parse_dates=["date"])
print(f"  ✅ Loaded {len(df):,} rows")


# ─────────────────────────────────────────────────────────
# DIMENSION TABLE 1: DimDate
# Contains all date-related attributes
# Why? So we can filter/group by year, month, quarter easily
# ─────────────────────────────────────────────────────────
print("\n[2/7] Building DimDate...")

dim_date = df[["date"]].drop_duplicates().copy()
dim_date["date_key"]       = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
dim_date["full_date"]      = dim_date["date"].dt.date.astype(str)
dim_date["year"]           = dim_date["date"].dt.year
dim_date["quarter"]        = dim_date["date"].dt.quarter
dim_date["month"]          = dim_date["date"].dt.month
dim_date["month_name"]     = dim_date["date"].dt.strftime("%B")
dim_date["week_of_year"]   = dim_date["date"].dt.isocalendar().week.astype(int)
dim_date["day_of_month"]   = dim_date["date"].dt.day
dim_date["day_of_week_num"]= dim_date["date"].dt.dayofweek + 1   # 1=Monday
dim_date["day_name"]       = dim_date["date"].dt.strftime("%A")
dim_date["is_weekend"]     = (dim_date["date"].dt.dayofweek >= 5).astype(int)

# Singapore fiscal year (April to March)
dim_date["fiscal_year"] = dim_date.apply(
    lambda r: r["year"] if r["month"] >= 4 else r["year"] - 1, axis=1
)

dim_date = dim_date.drop(columns=["date"]).reset_index(drop=True)
dim_date.to_sql("DimDate", conn, if_exists="replace", index=False)
print(f"  ✅ DimDate created: {len(dim_date):,} unique dates")


# ─────────────────────────────────────────────────────────
# DIMENSION TABLE 2: DimStore
# Contains store and region info
# ─────────────────────────────────────────────────────────
print("\n[3/7] Building DimStore...")

dim_store = df[["store_name", "region"]].drop_duplicates().copy()
dim_store = dim_store.reset_index(drop=True)
dim_store["store_key"] = dim_store.index + 1

# Enrich with additional store attributes
store_sizes = {
    "Jurong Point Central": "Large",   "Bishan Junction": "Medium",
    "Plaza Singapura": "Large",        "Causeway Point": "Large",
    "Sembawang Shopping": "Medium",    "Northpoint City": "Large",
    "Compass One": "Medium",           "Hougang Mall": "Medium",
    "NEX Serangoon": "Large",          "Tampines Mall": "Large",
    "Changi City Point": "Medium",     "Eastpoint Mall": "Medium",
    "JEM Jurong": "Large",             "Westgate": "Large",
    "IMM Jurong": "Large",
}
tourist_areas = {
    "Jurong Point Central": 1, "Bishan Junction": 0, "Plaza Singapura": 1,
    "Causeway Point": 0, "Sembawang Shopping": 0, "Northpoint City": 0,
    "Compass One": 0, "Hougang Mall": 0, "NEX Serangoon": 0,
    "Tampines Mall": 0, "Changi City Point": 1, "Eastpoint Mall": 0,
    "JEM Jurong": 1, "Westgate": 1, "IMM Jurong": 1,
}
dim_store["store_size"]      = dim_store["store_name"].map(store_sizes).fillna("Medium")
dim_store["is_tourist_area"] = dim_store["store_name"].map(tourist_areas).fillna(0).astype(int)
dim_store["region_code"]     = dim_store["region"].str[:3].str.upper()

dim_store[["store_key", "store_name", "region", "region_code",
           "store_size", "is_tourist_area"]].to_sql(
    "DimStore", conn, if_exists="replace", index=False
)
print(f"  ✅ DimStore created: {len(dim_store):,} stores across {dim_store['region'].nunique()} regions")


# ─────────────────────────────────────────────────────────
# DIMENSION TABLE 3: DimProduct
# Contains product and category info
# ─────────────────────────────────────────────────────────
print("\n[4/7] Building DimProduct...")

dim_product = df[["product_name", "category"]].drop_duplicates().copy()
dim_product = dim_product.reset_index(drop=True)
dim_product["product_key"] = dim_product.index + 1

# Category metadata
cat_margins = {
    "Fresh Produce": 0.22, "Dairy & Eggs": 0.28, "Beverages": 0.32,
    "Snacks & Confectionery": 0.38, "Frozen Foods": 0.30,
    "Household Supplies": 0.35, "Personal Care": 0.42,
    "Bakery & Bread": 0.45, "Meat & Seafood": 0.25, "Rice & Grains": 0.18,
    "Condiments & Sauces": 0.33, "Baby Products": 0.40,
    "Health & Wellness": 0.48, "Ready Meals": 0.35, "Alcohol & Spirits": 0.45,
}
cat_groups = {
    "Fresh Produce": "Perishables", "Dairy & Eggs": "Perishables",
    "Meat & Seafood": "Perishables", "Bakery & Bread": "Perishables",
    "Beverages": "Non-Perishables", "Snacks & Confectionery": "Non-Perishables",
    "Frozen Foods": "Non-Perishables", "Rice & Grains": "Non-Perishables",
    "Ready Meals": "Non-Perishables", "Condiments & Sauces": "Non-Perishables",
    "Alcohol & Spirits": "Non-Perishables",
    "Household Supplies": "Non-Food", "Personal Care": "Non-Food",
    "Baby Products": "Non-Food", "Health & Wellness": "Non-Food",
}
dim_product["category_group"]  = dim_product["category"].map(cat_groups).fillna("Other")
dim_product["base_gp_margin"]  = dim_product["category"].map(cat_margins).fillna(0.30)
dim_product["is_fresh_item"]   = (dim_product["category_group"] == "Perishables").astype(int)

dim_product[["product_key", "product_name", "category",
             "category_group", "base_gp_margin", "is_fresh_item"]].to_sql(
    "DimProduct", conn, if_exists="replace", index=False
)
print(f"  ✅ DimProduct created: {len(dim_product):,} products across {dim_product['category'].nunique()} categories")


# ─────────────────────────────────────────────────────────
# DIMENSION TABLE 4: DimCustomer
# Customer segment and loyalty information
# ─────────────────────────────────────────────────────────
print("\n[5/7] Building DimCustomer...")

dim_customer = df[["customer_segment", "member_tier"]].drop_duplicates().copy()
dim_customer = dim_customer.reset_index(drop=True)
dim_customer["customer_key"] = dim_customer.index + 1

# Tier ranking (used for ordering in charts)
tier_rank = {"Non-Member": 1, "Plus": 2, "Silver": 3, "Gold": 4, "Platinum": 5}
dim_customer["tier_rank"] = dim_customer["member_tier"].map(tier_rank).fillna(1)
dim_customer["has_membership"] = (dim_customer["member_tier"] != "Non-Member").astype(int)

dim_customer.to_sql("DimCustomer", conn, if_exists="replace", index=False)
print(f"  ✅ DimCustomer created: {len(dim_customer):,} customer groups")


# ─────────────────────────────────────────────────────────
# DIMENSION TABLE 5: DimPromotion
# Promotion types and discount info
# ─────────────────────────────────────────────────────────
print("\n[6/7] Building FactSales (the main fact table)...")

# Build surrogate keys to link fact to dimensions
# SURROGATE KEY = a unique number we assign to connect tables

# Date key
df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)

# Store key
df = df.merge(
    dim_store[["store_key", "store_name", "region"]],
    on=["store_name", "region"], how="left"
)

# Product key
df = df.merge(
    dim_product[["product_key", "product_name", "category"]],
    on=["product_name", "category"], how="left"
)

# Customer key
df = df.merge(
    dim_customer[["customer_key", "customer_segment", "member_tier"]],
    on=["customer_segment", "member_tier"], how="left"
)

# Build FactSales — only include keys and measurable facts
fact_sales = df[[
    "transaction_id",          # Natural key (from source system)
    "date_key",                # FK → DimDate
    "store_key",               # FK → DimStore
    "product_key",             # FK → DimProduct
    "customer_key",            # FK → DimCustomer
    "hour",                    # Fact attribute
    "quantity",                # Measure
    "unit_price_sgd",          # Measure
    "discount_pct",            # Measure
    "unit_price_after_disc",   # Measure (derived)
    "subtotal_sgd",            # Measure
    "gst_rate",                # Measure
    "gst_amount_sgd",          # Measure
    "total_sgd",               # Measure ← the main KPI!
    "gross_profit_sgd",        # Measure
    "payment_method",          # Degenerate dimension
    "is_promotion",            # Flag
    "promo_type",              # Degenerate dimension
    "cdc_voucher_used",        # Flag
    "is_tourist_purchase",     # Flag
    "is_weekend",              # Flag
    "is_public_holiday",       # Flag
    "holiday_season_flag",     # Flag
    "bonus_period_flag",       # Flag
    "period_label",            # Degenerate dimension
    "gst_period",              # Degenerate dimension
    "inflation_multiplier",    # Analytical measure
]].copy()

fact_sales.to_sql("FactSales", conn, if_exists="replace", index=False)
print(f"  ✅ FactSales created: {len(fact_sales):,} transaction rows")

# Verify foreign key integrity
fk_issues = fact_sales[fact_sales["store_key"].isna()].shape[0]
print(f"  FK integrity check — Orphan records: {fk_issues}")


# ── Final Summary ─────────────────────────────────────────
print("\n" + "=" * 60)
print("  STAR SCHEMA BUILD SUMMARY")
print("=" * 60)
tables = ["DimDate", "DimStore", "DimProduct", "DimCustomer", "FactSales"]
for t in tables:
    count = pd.read_sql(f"SELECT COUNT(*) as n FROM {t}", conn).iloc[0, 0]
    print(f"  {t:<20} : {count:>8,} rows")

print("\n  Schema Diagram:")
print("          DimDate")
print("             |")
print("  DimStore ——— FactSales ——— DimProduct")
print("             |")
print("         DimCustomer")
print()
print("  ✅ STAR SCHEMA COMPLETE! Run elt/transform.py next.")
conn.close()
