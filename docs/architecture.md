# 🏗️ System Architecture Documentation
## Giant Supermart Singapore — Data Engineering Pipeline

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                               │
│  CSV: giant_supermart_120k.csv (120,000 rows · 34 columns)       │
└─────────────────────────┬────────────────────────────────────────┘
                           │  pandas read_csv()
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                                │
│  ingestion/ingest.py                                              │
│  • Loads CSV into SQLite (dev) / PostgreSQL (prod)                │
│  • Validates columns, types, duplicates                           │
│  • Creates: raw_transactions table                                │
└─────────────────────────┬────────────────────────────────────────┘
                           │  SQL DDL + pandas to_sql()
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│               DATA WAREHOUSE — STAR SCHEMA                        │
│  warehouse/create_star_schema.py                                  │
│                                                                   │
│      DimDate ──────────────────────────────┐                     │
│      DimStore ─────┐                       │                     │
│      DimProduct ───┼─── FactSales ─────────┘                     │
│      DimCustomer ──┘         │                                   │
│                              │ (degenerate dims in fact)         │
│                              └── payment_method, gst_period...   │
└─────────────────────────┬────────────────────────────────────────┘
                           │  SQL queries via pandas read_sql()
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                    ELT TRANSFORM LAYER                            │
│  elt/transform.py                                                 │
│                                                                   │
│  Creates Marts (pre-aggregated business tables):                  │
│  • mart_monthly_sales      Monthly revenue trends                 │
│  • mart_regional_sales     Store & region performance             │
│  • mart_category_profit    Category profitability matrix          │
│  • mart_customer_ltv       Customer lifetime value                │
│  • mart_product_ranking    Top products + margins                 │
│  • mart_cdc_impact         CDC voucher analysis                   │
│  • mart_gst_analysis       GST transition analysis                │
└─────────────────────────┬────────────────────────────────────────┘
                           │  Custom Python tests
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                  DATA QUALITY LAYER                               │
│  quality/quality_checks.py                                        │
│                                                                   │
│  Tests run:                                                       │
│  ✅ NULL value checks (8 critical columns)                        │
│  ✅ Duplicate transaction detection                               │
│  ✅ Range validation (prices, quantities, GST rates)              │
│  ✅ Referential integrity (all FK checks)                         │
│  ✅ Business logic (totals reconciliation)                        │
│  ✅ Distribution checks (all regions/years present)               │
└─────────────────────────┬────────────────────────────────────────┘
                           │  pandas + NumPy + Matplotlib
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                   ANALYSIS LAYER                                  │
│  analysis/eda.py + notebooks/analysis.ipynb                       │
│                                                                   │
│  Insights generated:                                              │
│  • Revenue trends + YoY growth                                    │
│  • Regional breakdown + tourist impact                            │
│  • Category profitability matrix                                  │
│  • Customer segmentation + LTV                                    │
│  • CDC voucher basket lift analysis                               │
│  • GST transition cost analysis                                   │
│  • Peak hour footfall patterns                                    │
└─────────────────────────┬────────────────────────────────────────┘
                           │  Streamlit
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                BUSINESS DASHBOARD                                 │
│  streamlit_app/app.py                                             │
│                                                                   │
│  6 pages: Overview · Regional · Products · Customers · Policy     │
│  Features: Interactive filters, Plotly charts, Download exports   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Technology Decisions

### Why SQLite (not PostgreSQL)?
| Criterion         | SQLite (chosen for dev)     | PostgreSQL (production) |
|-------------------|-----------------------------|-------------------------|
| Setup time        | Zero — file-based           | Requires installation   |
| Performance (120K)| Excellent (<1 second)       | Excellent               |
| Portability       | Single `.db` file           | Requires server         |
| Migration effort  | Change 1 connection string  | N/A                     |
**Decision**: Use SQLite locally, PostgreSQL in production — same SQL works for both.

### Why Star Schema?
| Design            | Description                   | Best For |
|-------------------|-------------------------------|----------|
| **Star Schema** ✅ | Fact + denormalised dims      | Analytics, BI dashboards |
| Snowflake Schema  | Normalised dims               | Storage-critical systems |
| 3NF (OLTP)       | Fully normalised              | Transactional systems    |

**Decision**: Star schema chosen because:
1. Query performance — fewer JOINs needed
2. Simplicity — business users can understand it
3. Perfect for aggregations (SUM, GROUP BY)
4. Industry standard for data warehouses (Redshift, BigQuery, Snowflake)

### Why Streamlit (not Power BI / Tableau)?
- **Free and open-source** — no licence cost
- **Python-native** — same language as pipeline
- **Deployable in minutes** — `streamlit run app.py`
- **Customisable** — full Python control over logic

### Why Custom Quality Tests (not Great Expectations)?
- Great Expectations adds significant setup complexity
- Custom tests are transparent, readable, and fast
- Same logic — null, duplicate, range, business rule checks
- Easier to explain in a student project context

---

## 3. Star Schema Design Justification

### FactSales (Grain: one row per transaction)
Measures stored: quantity, prices, discounts, taxes, gross profit, flags

### DimDate (Role: time intelligence)
Allows easy GROUP BY year/quarter/month/week without string parsing

### DimStore (Role: geographic analysis)
Enriched with store_size, is_tourist_area, region_code

### DimProduct (Role: product analysis)
Enriched with category_group, is_fresh_item, base_gp_margin

### DimCustomer (Role: customer segmentation)
Combines member_tier + customer_segment — enables RFM-style analysis

---

## 4. Data Lineage

```
raw_transactions (CSV)
    → DimDate (date fields extracted)
    → DimStore (region + store fields)
    → DimProduct (category + product fields)
    → DimCustomer (segment + tier fields)
    → FactSales (all measures + FK references to dims)
        → mart_monthly_sales   (GROUP BY year, month)
        → mart_regional_sales  (GROUP BY region, store)
        → mart_category_profit (GROUP BY category)
        → mart_customer_ltv    (GROUP BY tier, segment)
        → mart_product_ranking (GROUP BY product)
        → mart_cdc_impact      (GROUP BY period, voucher)
        → mart_gst_analysis    (GROUP BY gst_period)
```

---

## 5. Pipeline Execution Order

```bash
# 1. Ingest raw data
python ingestion/ingest.py

# 2. Build star schema
python warehouse/create_star_schema.py

# 3. Run ELT transforms (build marts)
python elt/transform.py

# 4. Validate data quality
python quality/quality_checks.py

# 5. Generate analysis + charts
python analysis/eda.py

# 6. Launch dashboard
streamlit run streamlit_app/app.py
```

---

*Architecture Document v1.0 | Giant Supermart Data Engineering Project*
