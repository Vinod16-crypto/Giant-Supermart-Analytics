"""
=============================================================
STEP 3: ELT PIPELINE — TRANSFORMATIONS
File: elt/transform.py

PURPOSE:
  ELT = Extract, Load, Transform
  (Note: We loaded FIRST, then transform IN the database)
  
  This is different from ETL (Extract, Transform, Load)
  where you transform BEFORE loading.
  
  Modern data engineering prefers ELT because:
  - The warehouse handles compute (it's powerful)
  - Raw data is preserved for reprocessing
  - Transformations are auditable

WHAT WE BUILD HERE:
  1. mart_monthly_sales    → Monthly revenue trends
  2. mart_regional_sales   → Performance by Singapore region
  3. mart_category_profit  → Category profitability matrix
  4. mart_customer_ltv     → Customer lifetime value estimates
  5. mart_product_ranking  → Top products by revenue + margin
  6. mart_cdc_impact       → CDC voucher effect analysis
  7. mart_gst_analysis     → GST transition impact

BEGINNER TIP:
  "Mart" = data mart = a summarised table built for a specific
  business question. Think of it like a pre-calculated report.

HOW TO RUN:
  python elt/transform.py
=============================================================
"""

import pandas as pd
import sqlite3
import os

DB_PATH = "data/giant_supermart.db"

print("=" * 60)
print("  GIANT SUPERMART — ELT TRANSFORMATION PIPELINE")
print("=" * 60)

conn = sqlite3.connect(DB_PATH)


# ─────────────────────────────────────────────────────────
# MART 1: Monthly Sales Trends
# Business question: How does revenue change month by month?
# ─────────────────────────────────────────────────────────
print("\n[1/7] Building mart_monthly_sales...")

sql = """
SELECT
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    COUNT(f.transaction_id)                              AS transaction_count,
    SUM(f.quantity)                                      AS total_units_sold,
    ROUND(SUM(f.total_sgd), 2)                           AS total_revenue,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_basket_size,
    ROUND(SUM(f.gross_profit_sgd), 2)                    AS gross_profit,
    ROUND(SUM(f.gross_profit_sgd) / SUM(f.subtotal_sgd) * 100, 2) AS gp_margin_pct,
    SUM(f.is_promotion)                                  AS promo_transactions,
    SUM(f.cdc_voucher_used)                              AS cdc_redemptions,
    SUM(f.holiday_season_flag)                           AS holiday_txns,
    ROUND(AVG(f.gst_rate) * 100, 1)                     AS avg_gst_rate_pct
FROM FactSales f
JOIN DimDate d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, d.quarter
ORDER BY d.year, d.month
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_monthly_sales", conn, if_exists="replace", index=False)
print(f"  ✅ mart_monthly_sales: {len(mart)} months")


# ─────────────────────────────────────────────────────────
# MART 2: Regional Performance
# Business question: Which region performs best?
# ─────────────────────────────────────────────────────────
print("\n[2/7] Building mart_regional_sales...")

sql = """
SELECT
    s.region,
    s.store_name,
    s.store_size,
    s.is_tourist_area,
    COUNT(f.transaction_id)                              AS transactions,
    ROUND(SUM(f.total_sgd), 2)                           AS total_revenue,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_basket,
    ROUND(SUM(f.gross_profit_sgd), 2)                    AS gross_profit,
    ROUND(SUM(f.gross_profit_sgd) / SUM(f.subtotal_sgd) * 100, 2) AS gp_margin_pct,
    SUM(f.is_tourist_purchase)                           AS tourist_txns,
    ROUND(SUM(CASE WHEN f.is_tourist_purchase=1 THEN f.total_sgd ELSE 0 END), 2) AS tourist_revenue,
    SUM(f.cdc_voucher_used)                              AS cdc_redemptions,
    SUM(CASE WHEN f.is_weekend=1 THEN 1 ELSE 0 END)     AS weekend_txns
FROM FactSales f
JOIN DimStore s ON f.store_key = s.store_key
GROUP BY s.region, s.store_name, s.store_size, s.is_tourist_area
ORDER BY total_revenue DESC
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_regional_sales", conn, if_exists="replace", index=False)
print(f"  ✅ mart_regional_sales: {len(mart)} store records")


# ─────────────────────────────────────────────────────────
# MART 3: Category Profitability
# Business question: Which categories make the most money?
# ─────────────────────────────────────────────────────────
print("\n[3/7] Building mart_category_profit...")

sql = """
SELECT
    p.category,
    p.category_group,
    p.is_fresh_item,
    COUNT(f.transaction_id)                              AS transactions,
    SUM(f.quantity)                                      AS total_units,
    ROUND(SUM(f.total_sgd), 2)                           AS total_revenue,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_transaction_value,
    ROUND(SUM(f.gross_profit_sgd), 2)                    AS gross_profit,
    ROUND(SUM(f.gross_profit_sgd) / SUM(f.subtotal_sgd) * 100, 2) AS gp_margin_pct,
    ROUND(AVG(f.discount_pct) * 100, 1)                 AS avg_discount_pct,
    SUM(f.is_promotion)                                  AS promo_transactions,
    ROUND(CAST(SUM(f.is_promotion) AS FLOAT) / COUNT(*) * 100, 1) AS promo_rate_pct
FROM FactSales f
JOIN DimProduct p ON f.product_key = p.product_key
GROUP BY p.category, p.category_group, p.is_fresh_item
ORDER BY total_revenue DESC
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_category_profit", conn, if_exists="replace", index=False)
print(f"  ✅ mart_category_profit: {len(mart)} categories")


# ─────────────────────────────────────────────────────────
# MART 4: Customer Lifetime Value (LTV)
# Business question: Which customer types are most valuable?
# ─────────────────────────────────────────────────────────
print("\n[4/7] Building mart_customer_ltv...")

sql = """
SELECT
    c.member_tier,
    c.customer_segment,
    c.has_membership,
    c.tier_rank,
    COUNT(f.transaction_id)                              AS total_transactions,
    ROUND(SUM(f.total_sgd), 2)                           AS total_spend,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_basket_size,
    ROUND(MAX(f.total_sgd), 2)                           AS max_single_transaction,
    ROUND(SUM(f.gross_profit_sgd), 2)                    AS total_gp_generated,
    SUM(f.cdc_voucher_used)                              AS voucher_uses,
    SUM(f.is_promotion)                                  AS promo_purchases,
    ROUND(CAST(SUM(f.is_promotion) AS FLOAT)/COUNT(*)*100, 1) AS promo_sensitivity_pct
FROM FactSales f
JOIN DimCustomer c ON f.customer_key = c.customer_key
GROUP BY c.member_tier, c.customer_segment, c.has_membership, c.tier_rank
ORDER BY avg_basket_size DESC
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_customer_ltv", conn, if_exists="replace", index=False)
print(f"  ✅ mart_customer_ltv: {len(mart)} customer segments")


# ─────────────────────────────────────────────────────────
# MART 5: Product Rankings
# Business question: What are our best and worst products?
# ─────────────────────────────────────────────────────────
print("\n[5/7] Building mart_product_ranking...")

sql = """
SELECT
    p.product_name,
    p.category,
    p.category_group,
    COUNT(f.transaction_id)                              AS times_purchased,
    SUM(f.quantity)                                      AS total_units_sold,
    ROUND(SUM(f.total_sgd), 2)                           AS total_revenue,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_revenue_per_txn,
    ROUND(SUM(f.gross_profit_sgd), 2)                    AS gross_profit,
    ROUND(SUM(f.gross_profit_sgd) / SUM(f.subtotal_sgd) * 100, 2) AS gp_margin_pct,
    ROUND(AVG(f.discount_pct) * 100, 1)                 AS avg_discount_given
FROM FactSales f
JOIN DimProduct p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.category_group
ORDER BY total_revenue DESC
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_product_ranking", conn, if_exists="replace", index=False)
print(f"  ✅ mart_product_ranking: {len(mart)} products")


# ─────────────────────────────────────────────────────────
# MART 6: CDC Voucher Impact
# Business question: How much do government vouchers help?
# ─────────────────────────────────────────────────────────
print("\n[6/7] Building mart_cdc_impact...")

sql = """
SELECT
    f.period_label,
    f.cdc_voucher_used,
    f.gst_period,
    COUNT(*)                                             AS transactions,
    ROUND(SUM(f.total_sgd), 2)                           AS total_revenue,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_basket,
    ROUND(SUM(f.gross_profit_sgd), 2)                    AS gross_profit
FROM FactSales f
GROUP BY f.period_label, f.cdc_voucher_used, f.gst_period
ORDER BY total_revenue DESC
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_cdc_impact", conn, if_exists="replace", index=False)
print(f"  ✅ mart_cdc_impact: {len(mart)} period segments")


# ─────────────────────────────────────────────────────────
# MART 7: GST Analysis
# Business question: How did GST changes affect basket size?
# ─────────────────────────────────────────────────────────
print("\n[7/7] Building mart_gst_analysis...")

sql = """
SELECT
    f.gst_period,
    f.gst_rate,
    COUNT(*)                                             AS transactions,
    ROUND(AVG(f.total_sgd), 2)                           AS avg_basket,
    ROUND(AVG(f.gst_amount_sgd), 2)                      AS avg_gst_paid,
    ROUND(SUM(f.gst_amount_sgd), 2)                      AS total_gst_collected,
    ROUND(SUM(f.total_sgd), 2)                           AS total_revenue,
    ROUND(SUM(f.gross_profit_sgd) / SUM(f.subtotal_sgd) * 100, 2) AS gp_margin_pct
FROM FactSales f
GROUP BY f.gst_period, f.gst_rate
ORDER BY f.gst_rate
"""
mart = pd.read_sql(sql, conn)
mart.to_sql("mart_gst_analysis", conn, if_exists="replace", index=False)
print(f"  ✅ mart_gst_analysis: {len(mart)} GST period segments")


# ── Print all marts created ───────────────────────────────
print("\n" + "=" * 60)
print("  ELT TRANSFORMATION SUMMARY")
print("=" * 60)
marts = [
    "mart_monthly_sales", "mart_regional_sales", "mart_category_profit",
    "mart_customer_ltv", "mart_product_ranking", "mart_cdc_impact",
    "mart_gst_analysis",
]
for m in marts:
    count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {m}", conn).iloc[0,0]
    print(f"  {m:<30} : {count:>5} rows")

print()
print("  ✅ ELT COMPLETE! Run quality/quality_checks.py next.")
conn.close()
