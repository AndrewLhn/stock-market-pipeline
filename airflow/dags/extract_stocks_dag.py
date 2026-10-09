import logging
import os
from datetime import datetime, timedelta
from io import StringIO

import boto3
import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_TICKERS = ("AAPL", "MSFT", "TSLA", "GOOGL", "AMZN", "NVDA", "META", "AMD", "NFLX", "BABA")


def configured_tickers() -> tuple[str, ...]:
    configured = os.getenv("STOCK_TICKERS", "")
    return tuple(ticker.strip().upper() for ticker in configured.split(",") if ticker.strip()) or DEFAULT_TICKERS


def extract_and_load_to_s3(ds: str, **_: object) -> None:
    logical_date = datetime.strptime(ds, "%Y-%m-%d")
    next_date = logical_date + timedelta(days=1)
    frames: list[pd.DataFrame] = []

    for ticker in configured_tickers():
        logging.info("Fetching %s for %s", ticker, ds)
        try:
            frame = yf.download(
                ticker,
                start=logical_date.strftime("%Y-%m-%d"),
                end=next_date.strftime("%Y-%m-%d"),
                progress=False,
            )
        except Exception:
            logging.exception("Unable to fetch %s for %s", ticker, ds)
            continue

        if frame.empty:
            logging.info("No market data for %s on %s", ticker, ds)
            continue

        if isinstance(frame.columns, pd.MultiIndex):
            frame.columns = frame.columns.droplevel(-1)
        frame = frame.reset_index()
        frame.columns = [str(column).strip() for column in frame.columns]
        frame["ticker"] = ticker
        frames.append(frame)

    if not frames:
        raise AirflowSkipException(f"No market data available for {ds}")

    payload = pd.concat(frames, ignore_index=True)
    year, month, day = ds.split("-")
    object_key = f"raw/year={year}/month={month}/day={day}/stocks.csv"

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    s3.put_object(
        Bucket=os.getenv("STOCK_DATA_BUCKET", "stock-market-data"),
        Key=object_key,
        Body=payload.to_csv(index=False),
        ContentType="text/csv",
    )
    logging.info("Wrote %s rows to %s", len(payload), object_key)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="Daily, idempotent market-data ingestion and dbt quality gate.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["market-data", "batch", "dbt"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_and_load_to_object_storage",
        python_callable=extract_and_load_to_s3,
    )
    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt_project && dbt build --profiles-dir .",
    )

    extract_task >> dbt_build_task
