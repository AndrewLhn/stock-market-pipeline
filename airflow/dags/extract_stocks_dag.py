from datetime import datetime, timedelta
import logging
import os
from io import StringIO
import pandas as pd
import boto3
import yfinance as yf

from airflow import DAG
from airflow.operators.python import PythonOperator

TICKERS = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'AMZN', 'NVDA', 'META', 'AMD', 'NFLX', 'BABA']

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),  # Симуляция начнется с 1 июня 2026 года
    'retries': 1,
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
    
