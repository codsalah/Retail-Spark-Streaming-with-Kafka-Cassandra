import os
import glob
import logging
from datetime import datetime
from typing import Dict, List, Any

# This code is to be used in the DAGs to validate the silver data
# It is not a DAG itself it is to be imported in the DAGs 


def setup_logging():
    """Setup logging for DAG utilities"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def check_file_exists(file_path: str) -> bool:
    """Check if a file exists"""
    return os.path.exists(file_path)

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB"""
    if os.path.exists(file_path):
        return round(os.path.getsize(file_path) / (1024 * 1024), 2) # Convert bytes to MB
    return 0.0

def get_csv_record_count(file_path: str) -> int:
    """Get number of records in CSV file (excluding header)"""
    try:
        with open(file_path, 'r') as f:
            return max(0, sum(1 for line in f) - 1)
    except Exception:
        return 0

def validate_silver_data_structure() -> Dict[str, Any]:
    """Validate the structure of silver data"""
    logger = setup_logging()
    
    silver_base_path = '/opt/airflow/dags/../data/silver'
    expected_tables = ['clickstream', 'purchases', 'customers']
    
    validation_result = {
        'is_valid': True,
        'tables': {},
        'errors': []
    }
    
    for table in expected_tables:
        table_path = os.path.join(silver_base_path, table)
        table_info = {
            'exists': os.path.exists(table_path),
            'csv_files': [],
            'total_records': 0,
            'total_size_mb': 0.0
        }
        
        if table_info['exists']:
            csv_files = glob.glob(f"{table_path}/*.csv")
            table_info['csv_files'] = [os.path.basename(f) for f in csv_files]
            
            for csv_file in csv_files:
                table_info['total_records'] += get_csv_record_count(csv_file)
                table_info['total_size_mb'] += get_file_size_mb(csv_file)
            
            if not csv_files:
                validation_result['errors'].append(f"No CSV files found in {table}")
                validation_result['is_valid'] = False
            
            if table_info['total_records'] == 0:
                validation_result['errors'].append(f"No records found in {table}")
                validation_result['is_valid'] = False
        else:
            validation_result['errors'].append(f"Table directory missing: {table}")
            validation_result['is_valid'] = False
        
        validation_result['tables'][table] = table_info
    
    return validation_result

def get_expected_columns() -> Dict[str, List[str]]:
    """Get expected columns for each silver table"""
    return {
        'clickstream': [
            'user_id', 'timestamp', 'page_url', 'event_type', 'product_id',
            'session_id', 'processed_timestamp', 'event_date', 'event_hour',
            'event_year', 'event_month', 'event_day', 'data_source'
        ],
        'purchases': [
            'order_id', 'user_id', 'timestamp', 'product_id', 'quantity',
            'unit_price', 'total_amount', 'processed_timestamp', 'purchase_date',
            'purchase_hour', 'purchase_year', 'purchase_month', 'purchase_day',
            'data_source'
        ],
        'customers': [
            'customer_id', 'name', 'email', 'country', 'signup_date',
            'processed_timestamp', 'signup_year', 'signup_month', 'signup_day',
            'email_domain', 'data_source'
        ]
    }

def validate_column_structure() -> Dict[str, Any]:
    """Validate that silver tables have expected columns"""
    import pandas as pd
    
    logger = setup_logging()
    silver_base_path = '/opt/airflow/dags/../data/silver'
    expected_columns = get_expected_columns()
    
    validation_result = {
        'is_valid': True,
        'tables': {},
        'errors': []
    }
    
    for table, expected_cols in expected_columns.items():
        table_path = os.path.join(silver_base_path, table)
        csv_files = glob.glob(f"{table_path}/*.csv")
        
        table_info = {
            'has_files': len(csv_files) > 0,
            'columns_match': False,
            'actual_columns': [],
            'missing_columns': [],
            'extra_columns': []
        }
        
        if csv_files:
            try:
                # Read first CSV to check columns
                df = pd.read_csv(csv_files[0], nrows=0)  # Just read headers
                actual_cols = list(df.columns)
                table_info['actual_columns'] = actual_cols
                
                missing = set(expected_cols) - set(actual_cols)
                extra = set(actual_cols) - set(expected_cols)
                
                table_info['missing_columns'] = list(missing)
                table_info['extra_columns'] = list(extra)
                table_info['columns_match'] = len(missing) == 0 and len(extra) == 0
                
                if not table_info['columns_match']:
                    validation_result['is_valid'] = False
                    if missing:
                        validation_result['errors'].append(f"{table}: Missing columns {missing}")
                    if extra:
                        validation_result['errors'].append(f"{table}: Extra columns {extra}")
                
            except Exception as e:
                validation_result['errors'].append(f"{table}: Error reading CSV - {str(e)}")
                validation_result['is_valid'] = False
        else:
            validation_result['errors'].append(f"{table}: No CSV files found")
            validation_result['is_valid'] = False
        
        validation_result['tables'][table] = table_info
    
    return validation_result

def cleanup_old_silver_data(days_old: int = 7) -> None:
    """Clean up silver data files older than specified days"""
    logger = setup_logging()
    
    silver_base_path = '/opt/airflow/dags/../data/silver'
    cutoff_time = datetime.now().timestamp() - (days_old * 24 * 3600)
    
    for table in ['clickstream', 'purchases', 'customers']:
        table_path = os.path.join(silver_base_path, table)
        if os.path.exists(table_path):
            csv_files = glob.glob(f"{table_path}/*.csv")
            
            for csv_file in csv_files:
                file_time = os.path.getmtime(csv_file)
                if file_time < cutoff_time:
                    try:
                        os.remove(csv_file)
                        logger.info(f"Removed old file: {csv_file}")
                    except Exception as e:
                        logger.error(f"Error removing {csv_file}: {str(e)}")

def get_processing_stats() -> Dict[str, Any]:
    """Get processing statistics for silver data"""
    validation = validate_silver_data_structure()
    
    stats = {
        'timestamp': datetime.now().isoformat(),
        'total_tables': len(validation['tables']),
        'valid_tables': sum(1 for t in validation['tables'].values() if t['exists']),
        'total_records': sum(t['total_records'] for t in validation['tables'].values()),
        'total_size_mb': sum(t['total_size_mb'] for t in validation['tables'].values()),
        'is_healthy': validation['is_valid']
    }
    
    return stats

# Airflow utility functions
def notify_on_failure(context):
    """Callback function for task failure notifications"""
    logger = setup_logging()
    
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    
    logger.error(f"Task failed: {dag_id}.{task_id} on {execution_date}")
    

def notify_on_success(context):
    """Callback function for task success notifications"""
    logger = setup_logging()
    
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    
    logger.info(f"Task succeeded: {dag_id}.{task_id}")

# Common task configurations
COMMON_TASK_ARGS = {
    'on_failure_callback': notify_on_failure,
    'on_success_callback': notify_on_success
}
