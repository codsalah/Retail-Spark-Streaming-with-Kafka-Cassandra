import subprocess
import os
import sys

def run_duckdb_query(query, db_path="/tmp/dbt_db/retail_analytics.duckdb"):
    """Run a query using the DuckDB binary"""
    try:
        cmd = ["/opt/airflow/dbt/duckdb", db_path, "-c", query]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print(f"Query executed successfully")
            if result.stdout.strip():
                print(result.stdout)
            return True
        else:
            print(f"Query failed with return code {result.returncode}")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"Exception running query: {e}")
        return False

def create_staging_views():
    """Create staging views"""
    print("\nCreating staging views...")
    
    # Create customers staging view
    customers_query = """
    CREATE OR REPLACE VIEW stg_customers AS
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
        LOWER(email) AS email_clean,
        CURRENT_TIMESTAMP AS loaded_at
    FROM read_csv_auto('/opt/airflow/data/silver/customers/part-*.csv')
    WHERE customer_id IS NOT NULL;
    """
    
    print("Creating stg_customers view...")
    if not run_duckdb_query(customers_query):
        return False
    
    # Create purchases staging view
    purchases_query = """
    CREATE OR REPLACE VIEW stg_purchases AS
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
    WHERE order_id IS NOT NULL;
    """
    
    print("Creating stg_purchases view...")
    if not run_duckdb_query(purchases_query):
        return False
    
    # Create clickstream staging view
    clickstream_query = """
    CREATE OR REPLACE VIEW stg_clickstream AS
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
    WHERE user_id IS NOT NULL;
    """
    
    print("Creating stg_clickstream view...")
    if not run_duckdb_query(clickstream_query):
        return False
    
    return True

def create_mart_tables():
    """Create mart tables"""
    print("\nCreating mart tables...")
    
    # Create customer dimension
    dim_customers_query = """
    CREATE OR REPLACE TABLE dim_customers AS
    WITH customer_purchases AS (
        SELECT
            customer_id,
            COUNT(*) AS total_orders,
            SUM(total_amount) AS total_spent,
            AVG(total_amount) AS avg_order_value,
            MIN(purchase_date) AS first_purchase_date,
            MAX(purchase_date) AS last_purchase_date
        FROM stg_purchases
        GROUP BY customer_id
    )
    SELECT
        c.customer_id,
        c.name,
        c.email_clean AS email,
        c.email_domain,
        c.country,
        c.signup_date,
        
        -- Purchase metrics
        COALESCE(cp.total_orders, 0) AS total_orders,
        COALESCE(cp.total_spent, 0) AS total_spent,
        COALESCE(cp.avg_order_value, 0) AS avg_order_value,
        cp.first_purchase_date,
        cp.last_purchase_date,
        
        -- Customer segmentation
        CASE 
            WHEN cp.total_orders IS NULL THEN 'No Purchases'
            WHEN cp.total_orders = 1 THEN 'One-time'
            WHEN cp.total_orders BETWEEN 2 AND 5 THEN 'Regular'
            WHEN cp.total_orders > 5 THEN 'Frequent'
        END AS customer_segment,
        
        c.loaded_at
    FROM stg_customers c
    LEFT JOIN customer_purchases cp ON c.customer_id = cp.customer_id;
    """
    
    print("Creating dim_customers table...")
    if not run_duckdb_query(dim_customers_query):
        return False
    
    # Create purchase fact table
    fct_purchases_query = """
    CREATE OR REPLACE TABLE fct_purchases AS
    SELECT
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
        
        -- Customer information
        c.name,
        c.email,
        c.country,
        c.customer_segment,
        
        p.loaded_at
    FROM stg_purchases p
    LEFT JOIN dim_customers c ON p.customer_id = c.customer_id;
    """
    
    print("Creating fct_purchases table...")
    if not run_duckdb_query(fct_purchases_query):
        return False
    
    # Create daily sales summary
    daily_sales_query = """
    CREATE OR REPLACE TABLE daily_sales_summary AS
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
    FROM fct_purchases
    GROUP BY 
        purchase_date,
        purchase_year,
        purchase_month,
        purchase_day
    ORDER BY purchase_date DESC;
    """
    
    print("Creating daily_sales_summary table...")
    if not run_duckdb_query(daily_sales_query):
        return False
    
    return True

def show_results():
    """Show some results from the created tables"""
    print("\nResults Summary:")

    # Count records in each table
    tables = ['stg_customers', 'stg_purchases', 'stg_clickstream', 'dim_customers', 'fct_purchases', 'daily_sales_summary']

    for table in tables:
        query = f"SELECT COUNT(*) as count FROM {table};"
        print(f"\n{table}:")
        run_duckdb_query(query)

    # Show sample data from daily sales summary
    print("\nSample Daily Sales Summary (Top 5 days by revenue):")
    sample_query = """
    SELECT 
        purchase_date,
        total_orders,
        unique_customers,
        total_revenue,
        avg_order_value
    FROM daily_sales_summary 
    ORDER BY total_revenue DESC 
    LIMIT 5;
    """
    run_duckdb_query(sample_query)

def main():
    """Main execution function"""
    print("Starting manual DBT transformation using DuckDB...")

    # Create staging views
    if not create_staging_views():
        print("Failed to create staging views")
        sys.exit(1)

    # Create mart tables
    if not create_mart_tables():
        print("Failed to create mart tables")
        sys.exit(1)

    # Show results
    show_results()

    print("\nAll transformations completed successfully!")
    print(f"Database saved to: /tmp/dbt_db/retail_analytics.duckdb")

if __name__ == "__main__":
    main()
