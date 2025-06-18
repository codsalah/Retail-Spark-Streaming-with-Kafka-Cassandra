-- Daily sales summary table
-- Aggregated sales metrics by date
-- This model summarizes key sales and customer metrics per day

{{ config(materialized='table') }}

SELECT
    purchase_date,
    purchase_year,
    purchase_month,
    purchase_day,
    
    -- Sales metrics
    COUNT(*) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT product_id) AS unique_products,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    
    -- Customer metrics
    COUNT(DISTINCT CASE WHEN customer_segment = 'One-time' THEN customer_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN customer_segment IN ('Regular', 'Frequent') THEN customer_id END) AS returning_customers,
    
    CURRENT_TIMESTAMP AS loaded_at

FROM {{ ref('fct_purchases') }}
GROUP BY 
    purchase_date,
    purchase_year,
    purchase_month,
    purchase_day
ORDER BY purchase_date DESC
