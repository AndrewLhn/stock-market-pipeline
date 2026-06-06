
  
  create view "stock_market"."main"."stg_stocks__dbt_tmp" as (
    

with raw_data as (
    select *
    from read_csv_auto('s3://stock-market-data/raw/**/*.csv')
)

select
    cast(Date as date) as trading_date,
    cast(ticker as varchar) as ticker,
    cast(Open as float) as open_price,
    cast(High as float) as high_price,
    cast(Low as float) as low_price,
    cast(Close as float) as close_price,
    cast("Adj Close" as float) as adj_close_price,
    cast(Volume as bigint) as volume
from raw_data
where ticker is not null and Date is not null
  );
