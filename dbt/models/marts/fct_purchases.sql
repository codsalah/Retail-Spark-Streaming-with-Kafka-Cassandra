-- Purchase fact table
-- Contains all purchase transactions with customer details
-- -- Serves as the main fact table for analyzing sales, revenue, and customer behavior

{{ config(materialized='table') }}

SELECT
    -- Transactional details
    p.order_id,
    p.customer_id,
    p.product_id,
    p.purchase_timestamp,
    p.purchase_date,
    p.purchase_year,
    p.purchase_month,
    p.purchase_day,
    p.quantity,
    p.unit_price,
    p.total_amount,
    
    -- Customer attributes (from dimension table)
    c.name,
    c.email,
    c.country,
    c.customer_segment,
    
    -- ETL load timestamp for data freshness tracking
    p.loaded_at

FROM {{ ref('stg_purchases') }} p
LEFT JOIN {{ ref('dim_customers') }} c ON p.customer_id = c.customer_id
