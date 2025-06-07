from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'load_csvs_to_hdfs',
    default_args=default_args,
    description='Load CSV files to HDFS',
    schedule_interval=timedelta(minutes=1),
    catchup=False
)

# Create HDFS directories if they don't exist
create_hdfs_dirs = BashOperator(
    task_id='create_hdfs_dirs',
    bash_command='''
    echo "Creating HDFS directories..."
    docker exec namenode hdfs dfs -mkdir -p /bronze/clickstream
    docker exec namenode hdfs dfs -mkdir -p /bronze/customers
    docker exec namenode hdfs dfs -mkdir -p /bronze/purchases
    docker exec namenode hdfs dfs -mkdir -p /silver
    docker exec namenode hdfs dfs -mkdir -p /golden
    echo "Directories created successfully"
    ''',
    dag=dag
)

# Load clickstream data
load_clickstream = BashOperator(
    task_id='load_clickstream',
    bash_command='''
    echo "Loading clickstream data..."
    docker exec namenode hdfs dfs -test -e /bronze/clickstream/clickstream.csv && \
    docker exec namenode hdfs dfs -rm /bronze/clickstream/clickstream.csv || true
    docker cp /opt/airflow/dags/../data/clickstream.csv namenode:/tmp/clickstream.csv
    docker exec namenode hdfs dfs -put /tmp/clickstream.csv /bronze/clickstream/
    docker exec namenode rm /tmp/clickstream.csv
    echo "Clickstream data loaded successfully"
    ''',
    dag=dag
)

# Load customers data
load_customers = BashOperator(
    task_id='load_customers',
    bash_command='''
    echo "Loading customers data..."
    docker exec namenode hdfs dfs -test -e /bronze/customers/customers.csv && \
    docker exec namenode hdfs dfs -rm /bronze/customers/customers.csv || true
    docker cp /opt/airflow/dags/../data/customers.csv namenode:/tmp/customers.csv
    docker exec namenode hdfs dfs -put /tmp/customers.csv /bronze/customers/
    docker exec namenode rm /tmp/customers.csv
    echo "Customers data loaded successfully"
    ''',
    dag=dag
)

# Load purchases data
load_purchases = BashOperator(
    task_id='load_purchases',
    bash_command='''
    echo "Loading purchases data..."
    docker exec namenode hdfs dfs -test -e /bronze/purchases/purchases.csv && \
    docker exec namenode hdfs dfs -rm /bronze/purchases/purchases.csv || true
    docker cp /opt/airflow/dags/../data/purchases.csv namenode:/tmp/purchases.csv
    docker exec namenode hdfs dfs -put /tmp/purchases.csv /bronze/purchases/
    docker exec namenode rm /tmp/purchases.csv
    echo "Purchases data loaded successfully"
    ''',
    dag=dag
)

# Set task dependencies
create_hdfs_dirs >> [load_clickstream, load_customers, load_purchases]