-- Staging model for purchases data
-- Reads from actual silver layer CSV files

{{ config(materialized='view') }}

SELECT
    order_id,
    user_id AS customer_id,
    timestamp AS purchase_timestamp,
    product_id,
    quantity,
    unit_price,
    total_amount,
    processed_timestamp,
    purchase_date,
    purchase_hour,
    purchase_year,
    purchase_month,
    purchase_day,
    data_source,
    CURRENT_TIMESTAMP AS loaded_at
FROM read_csv_auto('/opt/airflow/data/silver/purchases/part-*.csv')
WHERE order_id IS NOT NULL
