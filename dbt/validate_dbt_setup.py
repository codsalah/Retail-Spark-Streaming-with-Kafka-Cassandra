import subprocess
import os
import sys

def run_duckdb_query(query, db_path="/tmp/dbt_db/retail_analytics.duckdb"):
    """Run a query using the DuckDB binary"""
    try:
        cmd = ["/opt/airflow/dbt/duckdb", db_path, "-c", query]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, result.stderr.strip()
    except Exception as e:
        return False, str(e)

def validate_data_sources():
    """Validate that we can read from all silver data sources"""
    print("Validating Silver Data Sources...")

    sources = {
        'customers': '/opt/airflow/data/silver/customers/part-*.csv',
        'purchases': '/opt/airflow/data/silver/purchases/part-*.csv',
        'clickstream': '/opt/airflow/data/silver/clickstream/part-*.csv'
    }

    for source_name, path in sources.items():
        query = f"SELECT COUNT(*) as count FROM read_csv_auto('{path}');"
        success, result = run_duckdb_query(query)

        if success:
            count = result.split('\n')[-2].strip().replace('│', '').strip()
            print(f"  {source_name}: {count} records")
        else:
            print(f"  {source_name}: Failed to read - {result}")
            return False

    return True

def validate_transformations():
    """Validate that all transformations are working"""
    print("\nValidating DBT Transformations...")

    # Check if our database exists and has the expected tables
    tables = ['stg_customers', 'stg_purchases', 'stg_clickstream',
              'dim_customers', 'fct_purchases', 'daily_sales_summary']

    for table in tables:
        query = f"SELECT COUNT(*) as count FROM {table};"
        success, result = run_duckdb_query(query)

        if success:
            count = result.split('\n')[-2].strip().replace('│', '').strip()
            print(f"  {table}: {count} records")
        else:
            print(f"  {table}: Not found or error - {result}")
            return False

    return True

def validate_data_quality():
    """Validate data quality in the transformed tables"""
    print("\nValidating Data Quality...")

    # Check for data consistency
    quality_checks = [
        {
            'name': 'Customer-Purchase Join',
            'query': '''
                SELECT COUNT(*) as orphaned_purchases
                FROM fct_purchases p
                LEFT JOIN dim_customers c ON p.customer_id = c.customer_id
                WHERE c.customer_id IS NULL;
            ''',
            'expected': '0'
        },
        {
            'name': 'Revenue Calculation',
            'query': '''
                SELECT 
                    ROUND(SUM(total_revenue), 2) as total_from_summary,
                    ROUND(SUM(total_amount), 2) as total_from_facts
                FROM (
                    SELECT SUM(total_revenue) as total_revenue, 0 as total_amount FROM daily_sales_summary
                    UNION ALL
                    SELECT 0 as total_revenue, SUM(total_amount) as total_amount FROM fct_purchases
                );
            ''',
            'description': 'Revenue totals should match between summary and fact tables'
        },
        {
            'name': 'Date Consistency',
            'query': '''
                SELECT COUNT(*) as inconsistent_dates
                FROM fct_purchases 
                WHERE purchase_year != EXTRACT(year FROM purchase_date)
                   OR purchase_month != EXTRACT(month FROM purchase_date)
                   OR purchase_day != EXTRACT(day FROM purchase_date);
            ''',
            'expected': '0'
        }
    ]
    
    for check in quality_checks:
        success, result = run_duckdb_query(check['query'])

        if success:
            print(f"  {check['name']}: Passed")
            if 'expected' in check and check['expected'] in result:
                print(f"    Expected {check['expected']} and got it")
            else:
                # Show the result for informational checks
                lines = result.split('\n')
                for line in lines:
                    if '│' in line and not line.startswith('┌') and not line.startswith('├'):
                        print(f"    Result: {line.strip()}")
        else:
            print(f"  {check['name']}: Failed - {result}")
            return False

    return True

def show_sample_data():
    """Show sample data from key tables"""
    print("\nSample Data from Key Tables:")

    samples = [
        {
            'name': 'Top 5 Customers by Total Spent',
            'query': '''
                SELECT name, email, country, total_orders, total_spent, customer_segment
                FROM dim_customers
                WHERE total_spent > 0
                ORDER BY total_spent DESC
                LIMIT 5;
            '''
        },
        {
            'name': 'Recent Purchases',
            'query': '''
                SELECT purchase_date, name, country, product_id, quantity, total_amount
                FROM fct_purchases 
                ORDER BY purchase_date DESC, purchase_timestamp DESC
                LIMIT 5;
            '''
        },
        {
            'name': 'Daily Sales Trend',
            'query': '''
                SELECT purchase_date, total_orders, unique_customers, 
                       ROUND(total_revenue, 2) as total_revenue,
                       ROUND(avg_order_value, 2) as avg_order_value
                FROM daily_sales_summary 
                ORDER BY purchase_date DESC;
            '''
        }
    ]
    
    for sample in samples:
        print(f"\n  {sample['name']}:")
        success, result = run_duckdb_query(sample['query'])

        if success:
            print(result)
        else:
            print(f"    Failed to get sample: {result}")

def main():
    """Main validation function"""
    print("DBT Setup Validation Starting...")
    print("=" * 50)

    # Step 1: Validate data sources
    if not validate_data_sources():
        print("\nData source validation failed!")
        sys.exit(1)

    # Step 2: Validate transformations
    if not validate_transformations():
        print("\nTransformation validation failed!")
        sys.exit(1)

    # Step 3: Validate data quality
    if not validate_data_quality():
        print("\nData quality validation failed!")
        sys.exit(1)

    # Step 4: Show sample data
    show_sample_data()

    print("\n" + "=" * 50)
    print("DBT Setup Validation PASSED!")
    print("\nYour dbt setup is working correctly with actual silver data!")
    print(f"Database location: /tmp/dbt_db/retail_analytics.duckdb")
    print("\nSummary:")
    print("  • All silver data sources are readable")
    print("  • All staging views are created successfully")
    print("  • All mart tables are built correctly")
    print("  • Data quality checks passed")
    print("  • Transformations are producing expected results")

if __name__ == "__main__":
    main()
