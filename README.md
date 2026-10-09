# Market Data Batch Pipeline

A local reference pipeline for daily equity-price ingestion. It writes immutable date partitions to
MinIO, builds DuckDB models with dbt, and stops the workflow when data-quality checks fail.

~~~text
yfinance -> Airflow -> MinIO (raw CSV partitions) -> dbt / DuckDB -> analytical mart
~~~

## What is implemented

- Daily Airflow DAG with retries and one active run at a time.
- Date-partitioned raw files in MinIO: `raw/year=YYYY/month=MM/day=DD/stocks.csv`.
- Re-running a logical date overwrites only that date's object, making ingestion idempotent.
- Configurable ticker universe through `STOCK_TICKERS`.
- dbt staging and fact models for OHLCV, turnover, daily return, and 3/7-day moving averages.
- dbt tests are executed with `dbt build` after successful ingestion.
- A no-data trading day skips downstream transformation instead of writing an empty partition.

## Local run

Requirements: Docker Compose and Docker.

~~~bash
cp .env.example .env
# Replace the placeholder passwords in .env.
docker compose up -d --build
~~~

Open Airflow at http://localhost:8080 and trigger `stock_market_pipeline`.
The pipeline reads its configuration from the environment:

| Variable | Meaning |
| --- | --- |
| `STOCK_TICKERS` | Comma-separated list of symbols |
| `STOCK_DATA_BUCKET` | MinIO bucket for raw partitions |
| `S3_ENDPOINT_URL` | Object-storage endpoint; defaults to the Compose MinIO service |

## Data model

| Model | Purpose |
| --- | --- |
| `stg_stocks` | Typed raw market records read from object storage |
| `fct_stock_performance` | Daily prices, turnover, returns, and rolling averages |

The current mart intentionally covers a small, explicit ticker universe. It is not investment
advice and should not be treated as a real-time market-data service.

## Operational notes

- The DAG has `catchup=False`. Use a deliberate Airflow backfill when historical dates must be
  replayed.
- dbt artifacts, local DuckDB files, logs, credentials, and Terraform state are ignored by Git.
- The repository currently supports local Docker Compose execution. Cloud deployment, alerting,
  and a managed serving layer are deliberately out of scope until they are implemented and tested.

## Next engineering increments

1. Add a source-freshness and ticker-completeness mart.
2. Add a parameterised backfill command with a reconciliation report.
3. Add CI that validates the DAG import and runs dbt against fixture data.
