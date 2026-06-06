

with raw_data as (
    select * from read_csv_auto('s3://stock-market-data/raw/**/*.csv')
),

cleaned as (
    select
        try_cast(Date as DATE) as trading_date,
        upper(ticker) as ticker,
        cast(Open as DOUBLE) as open_price,
        cast(High as DOUBLE) as high_price,
        cast(Low as DOUBLE) as low_price,
        cast(Close as DOUBLE) as close_price,
        cast(Volume as BIGINT) as volume
    from raw_data
)

select * from cleaned
where trading_date is not null