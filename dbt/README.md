
# dbt - Data Build Tool for Retail Analytics

This directory contains dbt models for transforming silver layer data into analytics-ready gold layer datasets.

## Project Overview

**Project Name**: `retail_analytics`
**Database**: DuckDB (local analytical database)
**Purpose**: Transform cleaned silver data into business-ready dimensional models and aggregated metrics

## Data Models

### Staging Layer (Silver → Staging)
- **stg_customers**: Cleaned customer data with email validation
- **stg_purchases**: Standardized purchase transactions
- **stg_clickstream**: Processed website interaction events

### Marts Layer (Staging → Gold)
- **dim_customers**: Customer dimension with segmentation and metrics
- **fct_purchases**: Purchase fact table with enriched attributes
- **daily_sales_summary**: Daily aggregated sales and customer metrics

## Running dbt

### Via Airflow (Recommended)
```bash
# Trigger dbt transformations DAG
docker exec airflow-container airflow dags trigger dbt_transformations_dag
```

### Manual Execution
```bash
# Run all models
docker exec airflow-container python /opt/airflow/dbt/run_dbt_manual.py

# Or run dbt commands directly
docker exec airflow-container dbt run --project-dir /opt/airflow/dbt
docker exec airflow-container dbt test --project-dir /opt/airflow/dbt
```

## Manual dbt Setup

You can also set up dbt manually outside of the Docker environment:

### 1. Install dbt
```bash
# Install dbt with DuckDB adapter
pip install dbt-duckdb

# Or install dbt core with specific adapters
pip install dbt-core dbt-duckdb
```

### 2. Initialize dbt Project
```bash
# Initialize new dbt project (if starting fresh)
dbt init retail_analytics

# Or use existing project
cd dbt/
```

### 3. Configure Profiles
Create `~/.dbt/profiles.yml`:
```yaml
retail_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: './duckdb/retail_analytics.duckdb'
      threads: 4
```

### 4. Run dbt Commands
```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Key Features

- **Incremental Models**: Efficient processing of large datasets
- **Data Quality Tests**: Automated validation of business rules
- **Documentation**: Auto-generated model documentation
- **Lineage Tracking**: Visual representation of data dependencies
- **Modular Design**: Reusable staging and mart layers

## Output

The dbt transformations produce analytics-ready tables in the gold layer:
- Customer segmentation and lifetime value analysis
- Purchase behavior and trends
- Daily sales performance metrics
- Data quality validation results
