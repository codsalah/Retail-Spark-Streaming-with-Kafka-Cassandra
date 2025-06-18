-- this file will deal with the silver layer of click_stream table
-- It will read the files from external source (opt/airflow/data/silver/clickstream)

{{ config(materialized='view') }}

SELECT
    user_id,
    timestamp,
    page_url,
    event_type,
    product_id,
    session_id,
    DATE(timestamp) AS event_date,
    EXTRACT(hour FROM timestamp) AS event_hour,
    EXTRACT(year FROM timestamp) AS event_year,
    EXTRACT(month FROM timestamp) AS event_month,
    EXTRACT(day FROM timestamp) AS event_day,
    CURRENT_TIMESTAMP AS processed_timestamp,
    'clickstream_file' AS data_source
FROM
    read_csv_auto('/opt/airflow/data/silver/clickstream/*.csv')
