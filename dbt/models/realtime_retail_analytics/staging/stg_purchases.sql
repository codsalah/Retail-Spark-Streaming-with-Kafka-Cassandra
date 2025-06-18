-- This file will deal with the silver layer of purchases
-- It will read the files from external source (opt/airflow/data/silver/purchases)


{{ config(materialized='view') }}

SELECT
  order_id,
  user_id,
  product_id,
  purchase_timestamp AS timestamp,
  quantity,
  unit_price,
  (quantity * unit_price) AS total_amount,
  DATE(purchase_timestamp) AS purchase_date,
  EXTRACT(hour FROM purchase_timestamp) AS purchase_hour,
  EXTRACT(year FROM purchase_timestamp) AS purchase_year,
  EXTRACT(month FROM purchase_timestamp) AS purchase_month,
  EXTRACT(day FROM purchase_timestamp) AS purchase_day,
  CURRENT_TIMESTAMP AS processed_timestamp,
  'purchases_file' AS data_source
FROM
  read_csv_auto('/opt/airflow/data/silver/purchases/*.csv')
