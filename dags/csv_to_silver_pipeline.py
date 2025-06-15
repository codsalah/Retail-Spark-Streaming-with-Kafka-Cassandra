from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

# Define the DAG
dag = DAG(
    'csv_to_silver_pipeline',
    default_args=default_args,
    description='Process CSV data to Silver layer using Spark',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['data-processing', 'spark', 'silver-layer']
)

def check_data_quality(**context):
    """Python function to check if silver data was created successfully"""
    import glob
    
    silver_dirs = [
        '/opt/airflow/dags/../data/silver/clickstream',
        '/opt/airflow/dags/../data/silver/purchases', 
        '/opt/airflow/dags/../data/silver/customers'
    ]
    
    for dir_path in silver_dirs:
        csv_files = glob.glob(f"{dir_path}/*.csv")
        if not csv_files:
            raise ValueError(f"No CSV files found in {dir_path}")
        
        # Check if files have content
        for csv_file in csv_files:
            if os.path.getsize(csv_file) == 0:
                raise ValueError(f"Empty file found: {csv_file}")
    
    print("✅ Data quality check passed - All silver files created successfully")
    return True

# Task 1: Start task
start_task = DummyOperator(
    task_id='start_csv_to_silver_processing',
    dag=dag
)

# Task 2: Check if source CSV files exist
check_source_files = BashOperator(
    task_id='check_source_csv_files',
    bash_command='''
    echo "Checking if source CSV files exist..."

    # Check for all required CSV files
    files=("clickstream.csv" "purchases.csv" "customers.csv")
    missing_files=()

    for file in "${files[@]}"; do
        filepath="/opt/airflow/dags/../data/$file"
        if [ ! -f "$filepath" ]; then
            missing_files+=("$file")
            echo "Missing file: $filepath XXXXXXXXXXX"
        else
            echo "Found file: $filepath ($(du -h "$filepath" | cut -f1)) %100"
        fi
    done

    if [ ${#missing_files[@]} -gt 0 ]; then
        echo "Missing files: ${missing_files[*]} XXXXXXXXXXXXXXXX"
        exit 1
    fi

    echo "All source CSV files are available %100"
    ''',
    dag=dag
)

# Task 3: Ensure silver directory exists
create_silver_directory = BashOperator(
    task_id='create_silver_directory',
    bash_command='''
    echo "Creating silver data directory..."
    mkdir -p /opt/airflow/dags/../data/silver
    echo "Silver directory created successfully"
    ''',
    dag=dag
)

# Task 4: Copy Spark script to container
copy_spark_script = BashOperator(
    task_id='copy_spark_script_to_container',
    bash_command='''
    echo "Copying Spark script to Spark master container..."
    docker exec spark-master mkdir -p /opt/spark/apps
    docker cp /opt/airflow/dags/../batch_csv_to_silver.py spark-master:/opt/spark/apps/
    echo "Spark script copied successfully"
    ''',
    dag=dag
)

# Task 5: Copy data to Spark container
copy_data_to_spark = BashOperator(
    task_id='copy_data_to_spark_container',
    bash_command='''
    echo "Copying data files to Spark master container..."
    docker cp /opt/airflow/dags/../data/ spark-master:/opt/spark/
    echo "Data files copied successfully"
    ''',
    dag=dag
)

# Task 6: Run Spark job for CSV to Silver processing
run_spark_job = BashOperator(
    task_id='run_csv_to_silver_spark_job',
    bash_command='''
    echo "Starting Spark job for CSV to Silver processing..."
    docker exec spark-master /spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --total-executor-cores 4 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.execution.arrow.pyspark.enabled=true \
        /opt/spark/apps/batch_csv_to_silver.py
    
    if [ $? -eq 0 ]; then
        echo "Spark job completed successfully %100"
    else
        echo "Spark job failed XXXXXXXXXXXXXXXXXX"
        exit 1
    fi
    ''',
    dag=dag
)

# Task 7: Copy silver data back to host
copy_silver_data_back = BashOperator(
    task_id='copy_silver_data_back_to_host',
    bash_command='''
    echo "Copying silver data back to host..."
    
    # Remove existing silver data to avoid conflicts
    rm -rf /opt/airflow/dags/../data/silver
    
    # Copy silver data from container
    docker cp spark-master:/opt/spark/data/silver/ /opt/airflow/dags/../data/
    
    echo "Silver data copied back successfully"
    echo "Silver data files created:"
    find /opt/airflow/dags/../data/silver -name "*.csv" -exec ls -la {} \;
    ''',
    dag=dag
)

# Task 8: Data quality validation
validate_silver_data = PythonOperator(
    task_id='validate_silver_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

# Task 8.1: Upload silver data to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_silver_data_to_hdfs',
    bash_command='''
    echo "Uploading silver data to HDFS..."

    # Create HDFS silver directories if they don't exist
    docker exec namenode hadoop fs -mkdir -p /silver/clickstream
    docker exec namenode hadoop fs -mkdir -p /silver/purchases
    docker exec namenode hadoop fs -mkdir -p /silver/customers

    # Remove existing data to avoid conflicts (ignore errors if directories are empty)
    docker exec namenode hadoop fs -rm -r -f /silver/clickstream/* || true
    docker exec namenode hadoop fs -rm -r -f /silver/purchases/* || true
    docker exec namenode hadoop fs -rm -r -f /silver/customers/* || true

    # Upload silver data to HDFS
    echo "Uploading clickstream data..."
    docker exec namenode hadoop fs -put /opt/airflow/dags/../data/silver/clickstream/*.csv /silver/clickstream/

    echo "Uploading purchases data..."
    docker exec namenode hadoop fs -put /opt/airflow/dags/../data/silver/purchases/*.csv /silver/purchases/

    echo "Uploading customers data..."
    docker exec namenode hadoop fs -put /opt/airflow/dags/../data/silver/customers/*.csv /silver/customers/

    echo "Silver data successfully uploaded to HDFS"

    # Verify upload
    echo "=== HDFS Silver Data Verification ==="
    echo "Clickstream files:"
    docker exec namenode hadoop fs -ls /silver/clickstream/
    echo ""
    echo "Purchases files:"
    docker exec namenode hadoop fs -ls /silver/purchases/
    echo ""
    echo "Customers files:"
    docker exec namenode hadoop fs -ls /silver/customers/
    ''',
    dag=dag
)

# Task 9: Generate processing summary
generate_summary = BashOperator(
    task_id='generate_processing_summary',
    bash_command='''
    echo "=== CSV to Silver Processing Summary ==="
    echo "Processing completed at: $(date)"
    echo ""
    echo "Silver data files created:"
    
    for table in clickstream purchases customers; do
        echo "--- $table ---"
        silver_dir="/opt/airflow/dags/../data/silver/$table"
        if [ -d "$silver_dir" ]; then
            csv_files=$(find "$silver_dir" -name "*.csv")
            if [ -n "$csv_files" ]; then
                for file in $csv_files; do
                    lines=$(wc -l < "$file")
                    size=$(du -h "$file" | cut -f1)
                    echo "  File: $(basename "$file")"
                    echo "  Records: $((lines-1))"  # Subtract header
                    echo "  Size: $size"
                done
            else
                echo "  No CSV files found"
            fi
        else
            echo "  Directory not found"
        fi
        echo ""
    done
    
    echo "=== Processing Complete ==="
    ''',
    dag=dag
)

# Task 10: Cleanup temporary files
cleanup_temp_files = BashOperator(
    task_id='cleanup_temporary_files',
    bash_command='''
    echo "Cleaning up temporary files in Spark container..."
    docker exec spark-master rm -rf /opt/spark/data/silver
    docker exec spark-master rm -f /opt/spark/apps/batch_csv_to_silver.py
    echo "Cleanup completed"
    ''',
    dag=dag
)

# Task 11: End task
end_task = DummyOperator(
    task_id='csv_to_silver_processing_complete',
    dag=dag
)

# Define task dependencies
start_task >> check_source_files >> create_silver_directory
create_silver_directory >> [copy_spark_script, copy_data_to_spark]
[copy_spark_script, copy_data_to_spark] >> run_spark_job
run_spark_job >> copy_silver_data_back
copy_silver_data_back >> validate_silver_data
validate_silver_data >> upload_to_hdfs
upload_to_hdfs >> generate_summary
generate_summary >> cleanup_temp_files
cleanup_temp_files >> end_task
