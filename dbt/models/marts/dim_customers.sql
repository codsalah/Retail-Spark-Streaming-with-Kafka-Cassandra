-- Customer dimension table
-- Contains customer information with purchase summary

{{ config(materialized='table') }}

-- CTE to aggregate purchase metrics per customer
WITH customer_purchases AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS total_spent,
        AVG(total_amount) AS avg_order_value,
        MIN(purchase_date) AS first_purchase_date,
        MAX(purchase_date) AS last_purchase_date
    FROM {{ ref('stg_purchases') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.name,
    c.email_clean AS email,
    c.email_domain,
    c.country,
    c.signup_date,
    
    -- Purchase metrics (null-safe using COALESCE to handle customers with no purchases)
    COALESCE(cp.total_orders, 0) AS total_orders,
    COALESCE(cp.total_spent, 0) AS total_spent,
    COALESCE(cp.avg_order_value, 0) AS avg_order_value,
    cp.first_purchase_date,
    cp.last_purchase_date,
    
    -- Customer segmentation based on purchase frequency
    CASE 
        WHEN cp.total_orders IS NULL THEN 'No Purchases'
        WHEN cp.total_orders = 1 THEN 'One-time'
        WHEN cp.total_orders BETWEEN 2 AND 5 THEN 'Regular'
        WHEN cp.total_orders > 5 THEN 'Frequent'
    END AS customer_segment,
    
    c.loaded_at

FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_purchases cp ON c.customer_id = cp.customer_id
