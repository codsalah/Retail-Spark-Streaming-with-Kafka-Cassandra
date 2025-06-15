#!/bin/bash

# CSV to Silver Data Processing Script
# This script submits the batch processing job to the Spark cluster

echo "=== CSV to Silver Data Processing ==="
echo "Starting Spark job submission..."

# Check if Docker containers are running
echo "Checking Docker containers..."
if ! docker ps | grep -q "spark-master"; then
    echo "Error: Spark master container is not running!"
    echo "Please start your Docker Compose setup first:"
    echo "  docker-compose up -d"
    exit 1
fi

# Create apps directory and copy the Python script to Spark master container
echo "Creating apps directory and copying Python script to Spark master..."
docker exec spark-master mkdir -p /opt/spark/apps
docker cp batch_csv_to_silver.py spark-master:/opt/spark/apps/

# Copy data directory to Spark master (if not already mounted)
echo "Ensuring data directory is accessible..."
docker cp data/ spark-master:/opt/spark/

# Submit the Spark job
echo "Submitting Spark job..."
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    /opt/spark/apps/batch_csv_to_silver.py

# Check if job completed successfully
if [ $? -eq 0 ]; then
    echo "Spark job completed successfully!"
    
    # Copy the silver data back to host
    echo "Copying silver data back to host..."
    docker cp spark-master:/opt/spark/data/silver/ ./data/
    
    echo "Silver data is now available in ./data/silver/"
    echo ""
    echo "Silver data files created:"
    ls -la data/silver/
    
else
    echo "Spark job failed! XXXXXXXXXXXXXXXXXxx"
    echo "Check the Spark UI at http://localhost:8080 for details"
    exit 1
fi

echo ""
echo "=== Processing Complete ==="
echo "You can now find your silver data in the data/silver/ directory"
echo "Each dataset has been cleaned and enhanced with additional columns:"
echo "  - clickstream: Added event_date, event_hour, data quality checks"
echo "  - purchases: Added total_amount, purchase_date, data validation"
echo "  - customers: Added email_domain, signup_date parsing, email validation"
