from datetime import datetime, timedelta
import json
import logging
import pandas as pd
import requests
import boto3
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

TICKERS = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'AMZN', 'NVDA', 'META', 'AMD', 'NFLX', 'BABA']

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1), # Начнем симуляцию с начала года
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load_to_s3(ds, **kwargs):
    date_obj = datetime.strptime(ds, '%Y-%m-%d')
    start_ts = int(date_obj.timestamp())
    end_ts = int((date_obj + timedelta(days=1)).timestamp())
    
    combined_data = []
    
    for ticker in TICKERS:
        logging.info(f"Fetching data for {ticker} for date {ds}")
        
        url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={start_ts}&period2={end_ts}&interval=1d&events=history&includeAdjustedClose=true"
        
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                df = pd.read_csv(StringIO(response.text))
                if not df.empty:
                    df['ticker'] = ticker 
                    combined_data.append(df)
            elif response.status_code == 404:
                logging.warning(f"No data found for {ticker} on {ds} (возможно, выходной на бирже)")
            else:
                logging.error(f"Failed to fetch {ticker}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"Error fetching {ticker}: {str(e)}")
            
    if not combined_data:
        logging.warning(f"No stock data fetched for {ds}. Пропускаем загрузку в S3.")
        return
        
    final_df = pd.concat(combined_data, ignore_index=True)
    
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio_admin',      
        aws_secret_access_key='minio_password_123',
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
    description='Извлечение данных акций из API и загрузка в S3 (MinIO)',
    schedule_interval='@daily', 
    catchup=True,               
    max_active_runs=3,          
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_load_to_s3',
        python_callable=extract_and_load_to_s3,
    )