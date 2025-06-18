-- Staging model for customers data
-- Reads from actual silver layer CSV files

{{ config(materialized='view') }}

SELECT
    customer_id,
    name,
    email,
    country,
    signup_date,
    processed_timestamp,
    signup_year,
    signup_month,
    signup_day,
    email_domain,
    data_source,
    -- Add some basic transformations
    LOWER(email) AS email_clean,
    CURRENT_TIMESTAMP AS loaded_at
FROM read_csv_auto('/opt/airflow/data/silver/customers/part-*.csv')
WHERE customer_id IS NOT NULL
