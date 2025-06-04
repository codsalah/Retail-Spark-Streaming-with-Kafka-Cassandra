from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'bronze_ingest_to_hdfs',
    default_args=default_args,
    schedule_interval='* * * * *',  # every minute
    catchup=False,
)

topics = ['clickstream', 'purchases', 'customers']

for topic in topics:
    BashOperator(
        task_id=f'ingest_{topic}_to_hdfs',
        bash_command=f'''
        HDFS_BASE="/bronze"
        LOCAL="/opt/airflow/host_data/{topic}.csv"
        HDFS="$HDFS_BASE/{topic}.csv"
        if [ -f "$LOCAL" ]; then
            if hdfs dfs -test -e "$HDFS"; then
                tail -n +2 "$LOCAL" | hdfs dfs -appendToFile - "$HDFS"
            else
                hdfs dfs -mkdir -p "$HDFS_BASE"
                hdfs dfs -copyFromLocal "$LOCAL" "$HDFS"
            fi
        fi
        ''',
        dag=dag,
    )