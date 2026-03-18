# 🛒 Giant Supermart Singapore — End-to-End Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![SQLite](https://img.shields.io/badge/Database-SQLite%2FPostgreSQL-green)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-red)
![pandas](https://img.shields.io/badge/Analysis-Pandas%20%7C%20NumPy-yellow)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

## 📌 Project Overview

A production-grade, end-to-end data engineering pipeline built on **120,000 rows** of synthetic retail transaction data from **Giant Supermart Singapore** — one of Singapore's most widely visited supermarket chains.

This project demonstrates the complete data lifecycle:
```
Raw CSV → Ingestion → Star Schema Warehouse → ELT Transform → 
Data Quality Tests → Python EDA → Business Insights → Streamlit Dashboard
```

---

## 🎯 Why Giant Supermart & This Dataset?

Giant Supermart was selected because:
- **All-class appeal**: Visited by families, professionals, elderly, tourists, and students across ALL Singapore regions
- **Rich temporal signals**: 3 years of data captures GST changes, CDC vouchers, festive seasons
- **Government policy observable**: CDC vouchers (2022–2024) and GST 7%→8%→9% transition provide rare natural experiments in retail
- **Singapore-specific relevance**: Covers all 5 planning regions (Central, North, North-East, East, West)
- **ML-ready**: 34 features across demographics, promotions, pricing, and behaviour

---

## 📂 Project Structure

```
giant_supermart_project/
│
├── data/                          # Raw source data
│   └── giant_supermart_120k.csv   # 120,000 transaction records
│
├── ingestion/                     # Step 1: Data Ingestion
│   ├── ingest.py                  # Load CSV → SQLite/PostgreSQL
│   └── schema_raw.sql             # Raw table DDL
│
├── warehouse/                     # Step 2: Star Schema Design
│   ├── create_star_schema.py      # Build Dim + Fact tables
│   └── star_schema.sql            # SQL DDL for all tables
│
├── elt/                           # Step 3: ELT Transformations
│   ├── transform.py               # Python ELT pipeline
│   └── transforms.sql             # SQL transformation queries
│
├── quality/                       # Step 4: Data Quality
│   ├── quality_checks.py          # Custom data quality tests
│   └── quality_report.py          # Generate quality report
│
├── analysis/                      # Step 5: Data Analysis
│   └── eda.py                     # Full EDA with pandas + numpy
│
├── notebooks/                     # Jupyter Notebooks
│   └── analysis.ipynb             # Interactive EDA notebook
│
├── streamlit_app/                 # Step 6: Dashboard
│   └── app.py                     # Streamlit dashboard
│
├── docs/                          # Documentation
│   ├── architecture.md            # System architecture guide
│   └── data_dictionary.md         # All column definitions
│
├── sql/                           # Analytical SQL queries
│   └── business_queries.sql       # Key business questions
│
├── requirements.txt               # Python dependencies
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```

---

## 🚀 Quick Start (5 Minutes)

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/giant-supermart-pipeline.git
cd giant-supermart-pipeline
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Full Pipeline
```bash
# Step 1: Ingest raw data into database
python ingestion/ingest.py

# Step 2: Build star schema
python warehouse/create_star_schema.py

# Step 3: Run ELT transformations
python elt/transform.py

# Step 4: Run data quality checks
python quality/quality_checks.py

# Step 5: Generate business analysis
python analysis/eda.py
```

### 4. Launch Dashboard
```bash
streamlit run streamlit_app/app.py
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                              │
│         CSV (120K rows) · 34 columns · 3 years              │
└─────────────────────┬───────────────────────────────────────┘
                      │ Python (pandas)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                  INGESTION LAYER                             │
│     ingest.py → raw_transactions table (SQLite/PostgreSQL)  │
└─────────────────────┬───────────────────────────────────────┘
                      │ SQL + Python ELT
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              DATA WAREHOUSE (Star Schema)                    │
│  DimDate  DimStore  DimProduct  DimCustomer  DimPromotion   │
│                    ↕                                         │
│                FactSales                                     │
└─────────────────────┬───────────────────────────────────────┘
                      │ Data Quality (custom tests)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              DATA QUALITY LAYER                              │
│   Null checks · Duplicate detection · Referential integrity  │
│   Business rule validation · Range checks                    │
└─────────────────────┬───────────────────────────────────────┘
                      │ pandas + numpy + SQLAlchemy
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              ANALYSIS & INSIGHTS                             │
│   Monthly trends · Regional analysis · Customer segments     │
│   CDC voucher impact · GST analysis · Seasonal patterns      │
└─────────────────────┬───────────────────────────────────────┘
                      │ Streamlit
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              BUSINESS DASHBOARD                              │
│        Interactive KPIs · Charts · Filters · Exports         │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Key Business Insights

| Insight | Finding |
|---------|---------|
| Top Region | Central (28% of revenue) |
| Peak Day | Saturday — 35% higher than weekdays |
| CDC Voucher Lift | +12–15% basket size during voucher windows |
| GST Impact | Average basket grew +22.5% from 7% to 9% era |
| Best Margin Category | Health & Wellness (48% GP margin) |
| Best Loyalty Tier | Platinum members spend 2.1× more per visit |
| Top Payment Method | NETS (22%) and GrabPay (19%) dominate |

---

## 🛠️ Tech Stack

| Layer | Tool | Why Chosen |
|-------|------|------------|
| Language | Python 3.10+ | Industry standard for data engineering |
| Database | SQLite (dev) / PostgreSQL (prod) | Zero-config for dev, enterprise-grade for prod |
| Data Processing | pandas + NumPy | Fast, flexible, widely used in industry |
| Visualisation | Plotly + Matplotlib | Interactive charts for dashboard |
| Dashboard | Streamlit | Rapid deployment of data apps |
| Quality Testing | Custom Python | Transparent, reproducible checks |
| Notebook | Jupyter | Exploratory analysis and storytelling |

---

## 👤 Author
**Vinod Vincent**  
*Data Engineering Project
