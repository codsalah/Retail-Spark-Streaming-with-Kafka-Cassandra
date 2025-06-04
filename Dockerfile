FROM apache/airflow:2.5.1

USER root

RUN mkdir -p /opt/airflow/config \
    && chown -R airflow: /opt/airflow/config

USER airflow

RUN pip install --no-cache-dir \
    "dbt-core" \
    "protobuf<3.21.0"