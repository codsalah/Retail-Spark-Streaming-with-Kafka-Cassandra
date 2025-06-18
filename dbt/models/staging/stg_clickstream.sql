-- Staging model for clickstream data
-- Reads from actual silver layer CSV files

{{ config(materialized='view') }}

SELECT
    user_id,
    timestamp AS event_timestamp,
    page_url,
    event_type,
    product_id,
    session_id,
    processed_timestamp,
    event_date,
    event_hour,
    event_year,
    event_month,
    event_day,
    data_source,
    CURRENT_TIMESTAMP AS loaded_at
FROM read_csv_auto('/opt/airflow/data/silver/clickstream/part-*.csv')
WHERE user_id IS NOT NULL
