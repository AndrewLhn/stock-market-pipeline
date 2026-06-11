INSERT INTO db_connection (id, name, engine, details, created_at, updated_at)
VALUES (
    99, 
    'Stock Market Analytics', 
    'postgres', 
    '{"host":"metabase-db","port":5432,"db":"metabase","user":"metabase_user","password":"metabase_pass_456","ssl":false}',
    NOW(), 
    NOW()
) ON CONFLICT (id) DO NOTHING;

INSERT INTO report_dashboard (id, name, description, creator_id, created_at, updated_at, parameters, archival)
VALUES (
    1, 
    'Мониторинг Рынка Акций (Топ-10)', 
    'Автоматический дашборд для отслеживания цен закрытия и скользящих средних', 
    1, NOW(), NOW(), '[]', false
) ON CONFLICT (id) DO NOTHING;

INSERT INTO report_card (id, name, dataset_query, display, description, creator_id, database_id, table_id, result_metadata, creator_id, created_at, updated_at)
VALUES (
    10, 
    'Динамика цен и Скользящие средние', 
    '{"type":"native","native":{"query":"SELECT trading_date, ticker, close_price, moving_avg_3d, moving_avg_7d FROM fct_stock_performance ORDER BY trading_date ASC","template-tags":{}},"database":99}', 
    'line', 
    'График цен закрытия с наложенными MA_3d и MA_7d', 
    1, 
    99, 
    NULL,
    '[{"name":"trading_date","display_name":"Дата","base_type":"type/Date"},{"name":"close_price","display_name":"Цена закрытия","base_type":"type/Float"}]',
    NOW(), 
    NOW()
) ON CONFLICT (id) DO NOTHING;

INSERT INTO report_dashboardcard (id, dashboard_id, card_id, row, col, size_x, size_y, parameter_bindings)
VALUES (
    100, 
    1,  
    10, 
    0, 
    0,  
    18, 
    10, 
    '[]'
) ON CONFLICT (id) DO NOTHING;