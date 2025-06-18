-- this file will deal with the silver layer of the customers table
-- It will read the files from external source (opt/airflow/data/silver/customers)


{{ config(materialized='view') }}

SELECT
  customer_id,
  first_name,
  last_name,
  email,
  split_part(email, '@', 2) AS email_domain,
  country,
  signup_date,
  'customers_file' AS data_source
FROM
  read_csv_auto('/opt/airflow/data/silver/customers/*.csv')
