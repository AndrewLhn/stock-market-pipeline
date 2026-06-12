# 📈 Automated End-to-End Stock Market Analytics Pipeline

This project implements a production-ready, automated data pipeline for financial market analytics using the **Modern Data Stack (MDS)**. It utilizes **Apache Airflow** for orchestration, **MinIO (S3)** as a Data Lake, **DuckDB + dbt** for high-performance computing, **PostgreSQL** as a serving layer, and **Metabase** for production-grade business intelligence.

The architecture strictly adheres to the **Medallion Architecture** design pattern, ensuring high reliability, strict data quality gating, and data-as-code idempotency.

---

## 🏗️ System Architecture & Data Flow
[ Financial API (yfinance) ]
              │
              ▼ (Apache Airflow Orchestration)
┌────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Data Lake)                              │
│  - MinIO S3 Bucket: `raw/year=/month=/day=/stocks.csv`  │
└────────────────────────────────────────────────────────┘
              │
              ▼ (dbt run)
┌────────────────────────────────────────────────────────┐
│  SILVER LAYER (Staging & Cleaning)                     │
│  - DuckDB: `stg_stocks` (Type casting & schema lock)   │
└────────────────────────────────────────────────────────┘
              │
              ▼ (dbt transformations & analytics)
┌────────────────────────────────────────────────────────┐
│  GOLD LAYER (Marts & Feature Engineering)              │
│  - DuckDB: `fct_stock_performance` (Moving Averages)   │
└────────────────────────────────────────────────────────┘
              │
              ▼ (dbt test - Data Quality Gate)
┌────────────────────────────────────────────────────────┐
│  DATA QUALITY CHECKS                                   │
│  - Schema rules, Unique constraints, Business validation│
└────────────────────────────────────────────────────────┘
              │
              ▼ (dbt `on-run-end` Hook Replication)
┌────────────────────────────────────────────────────────┐
│  SERVING LAYER (Relational Data Warehouse)             │
│  - PostgreSQL: Production Database                     │
└────────────────────────────────────────────────────────┘
              │
              ▼ (Live Querying)
 [ Metabase Dashboard: Market Trends BI ]

 ---

## 🔄 Core Workflow Pipeline

### 1. Data Extraction (Ingestion)
* **Apache Airflow** triggers daily cron jobs to pull stock metrics from external financial sources.
* Raw snapshots are saved as immutable CSV files into **MinIO S3 API** partitioned by `year/month/day` formatting. This serves as the **Bronze Layer** (Single Source of Truth).

### 2. Transformation Layer (dbt + DuckDB)
* **Silver Layer:** dbt standardizes naming conventions, strips metadata, casts timestamps, and builds decoupled staging views (`stg_stocks`).
* **Gold Layer:** Advanced transformations compute rolling 3-day and 7-day moving averages (`moving_avg_3d`, `moving_avg_7d`) alongside key performance indicators.

### 3. Data Quality Gate (CI/CD Automated Auditing)
* Production data is rigorously audited using integrated **dbt tests**.
* Enforced constraints include `not_null` validation on primary keys, `accepted_values` validation on tickers, and semantic tests checking that equity prices remain strictly positive.

### 4. Serving & BI Layer
* Upon a successful test run, an optimized `on-run-end` dbt macro mirrors data chunks to a remote **PostgreSQL** replica container.
* **Metabase** maps natively to PostgreSQL tables, running optimized visualization layers without taxing analytical resources inside the DuckDB container.

---

## 🛠️ Tech Stack & Infrastructure

* **Orchestration:** Apache Airflow v2.x
* **Storage (Object / OLAP):** MinIO (S3 API) & DuckDB (In-Memory Processing Engine)
* **Data Transformation:** dbt-core v1.11+ (with custom analytics macros)
* **Database (Serving Node):** PostgreSQL v15+
* **Data Visualization:** Metabase (Dashboard-as-Code ecosystem)
* **Containerization:** Docker & Docker Compose

---

## 🚀 Quick Start / Local Deployment

### Prerequisites
* Docker & Docker Compose installed on your local host engine.
* Python v3.10+ (for local environment venv testing).

### Step 1: Clone the Repo & Initialize Infrastructure

git clone [https://github.com/your-username/stock-market-pipeline.git](https://github.com/your-username/stock-market-pipeline.git)
cd stock-market-pipeline

# Spin up all microservices in background detached mode
docker-compose up -d

# Execute dbt Run inside Scheduler Node
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt run --profiles-dir ."

# Execute Automated Quality Tests
docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt test --profiles-dir ."