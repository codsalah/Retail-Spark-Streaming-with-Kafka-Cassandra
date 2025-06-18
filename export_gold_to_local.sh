#!/bin/bash
# Export Gold Data to Local CSV Files
# This script exports dbt gold layer data to local CSV files

set -e

echo "Exporting Gold Data to Local CSV Files..."
echo "=============================================="

# Check if container is running
if ! docker ps --format "table {{.Names}}" | grep -q "realtime-retail-analytics-kafka-spark-dbt-airflow-airflow-1"; then
    echo "Airflow container is not running!"
    echo "Please start it with: docker-compose up -d"
    exit 1
fi

# Create local gold directory
mkdir -p data/gold
echo "Created local directory: data/gold/"

# Run the export script inside the container
echo "Running export script inside container..."
docker exec realtime-retail-analytics-kafka-spark-dbt-airflow-airflow-1 bash -c "cd /opt/airflow/dbt && python export_gold_data.py"

# Copy the exported files to local
echo "Copying files to local directory..."
docker cp realtime-retail-analytics-kafka-spark-dbt-airflow-airflow-1:/tmp/gold_export/latest/. data/gold/

# Show what was exported
echo ""
echo "Export completed successfully!"
echo ""
echo "Exported Files:"
ls -lh data/gold/*.csv | while read line; do
    echo "   $line"
done

echo ""
echo "Summary:"
if [ -f "data/gold/export_summary.txt" ]; then
    cat data/gold/export_summary.txt
fi

echo ""
echo "Access your gold data at: ./data/gold/"
echo "   • dim_customers.csv - Customer dimension with purchase metrics"
echo "   • fct_purchases.csv - Purchase fact table with transaction details"
echo "   • daily_sales_summary.csv - Daily aggregated sales metrics"
echo ""
echo "To refresh the data, run this script again!"
