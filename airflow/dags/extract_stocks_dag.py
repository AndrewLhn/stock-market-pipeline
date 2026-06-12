import subprocess
import sys

try:
    import yfinance as yf
except ModuleNotFoundError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance"])
    import yfinance as yf

from datetime import datetime, timedelta
import logging
import os
from io import StringIO
import pandas as pd
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator  

TICKERS = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'AMZN', 'NVDA', 'META', 'AMD', 'NFLX', 'BABA']

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load_to_s3(ds, **kwargs):
    date_obj = datetime.strptime(ds, '%Y-%m-%d')
    next_day_obj = date_obj + timedelta(days=1)
    
    start_str = date_obj.strftime('%Y-%m-%d')
    end_str = next_day_obj.strftime('%Y-%m-%d')
    
    combined_data = []
    
    for ticker in TICKERS:
        logging.info(f"Fetching data for {ticker} for date {ds} via yfinance")
        try:
            df = yf.download(ticker, start=start_str, end=end_str, progress=False)
               
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.droplevel(1)
                
                df.columns = [str(col).strip() for col in df.columns]
                df = df.reset_index()
                df['ticker'] = ticker
                combined_data.append(df)
            else:
                logging.warning(f"No data returned for {ticker} on {ds} (скорее всего, выходной на бирже)")
        except Exception as e:
            logging.error(f"Error fetching data for {ticker}: {str(e)}")
            
    if not combined_data:
        logging.warning(f"No stock data fetched for any ticker on {ds}. Пропускаем загрузку в S3.")
        return
        
    final_df = pd.concat(combined_data, ignore_index=True)
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )
    
    year, month, day = ds.split('-')
    s3_key = f"raw/year={year}/month={month}/day={day}/stocks.csv"
    
    csv_buffer = StringIO()
    final_df.to_csv(csv_buffer, index=False)
    
    bucket_name = 'stock-market-data'
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    logging.info(f"Successfully uploaded data to s3://{bucket_name}/{s3_key}")

with DAG(
    'stock_market_pipeline',
    default_args=default_args,
    description='Извлечение данных акций через yfinance, трансформация в dbt и валидация качества данных',
    schedule_interval='@daily',
    catchup=True,  
    max_active_runs=2,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_load_to_s3',
        python_callable=extract_and_load_to_s3,
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt_project && dbt test --profiles-dir .',
    )

    extract_task >> dbt_run_task >> dbt_test_task