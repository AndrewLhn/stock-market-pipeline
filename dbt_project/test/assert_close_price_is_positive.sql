SELECT
    trading_date,
    ticker,
    close_price
FROM {{ ref('fct_stock_performance') }}
WHERE close_price <= 0