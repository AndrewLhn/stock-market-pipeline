Data Pipeline Architecture

The project follows a modern Medallion Architecture (Bronze → Silver → Gold) for processing financial market data.

Financial API
      │
      ▼
Airflow (Extract)
      │
      ▼
S3 Bucket: raw/                 ← Bronze Layer (Raw JSON/CSV data)
      │
      ▼
Airflow triggers dbt
      │
      ▼
DuckDB: staging models          ← Silver Layer (Data cleaning, validation, type casting)
      │
      ▼
dbt transformations
      │
      ▼
DuckDB: marts models            ← Gold Layer (Business-ready analytics and aggregations)
      │
      ▼
Metabase Dashboard              ← Reporting & Visualization Layer

Workflow

1. Data Extraction
    Apache Airflow extracts financial market data from external APIs.
    Raw data is stored in Amazon S3 in JSON/CSV format.
2. Bronze Layer
    Stores immutable raw source data.
    Serves as the single source of truth for downstream processing.
3. Silver Layer
    dbt staging models clean and standardize raw datasets.
    Data types, naming conventions, and quality checks are applied.
4. Gold Layer
    dbt mart models create business-ready datasets.
    Includes metrics such as moving averages, stock performance indicators, and top-performing assets.
5. Analytics
    Metabase connects directly to DuckDB marts.
    Interactive dashboards provide insights into market trends and stock performance.