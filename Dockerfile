FROM apache/airflow:2.5.1

USER root

# Install Hadoop client
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download and setup Hadoop client
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.1.3/hadoop-3.1.3.tar.gz \
    && tar -xzf hadoop-3.1.3.tar.gz -C /opt \
    && rm hadoop-3.1.3.tar.gz \
    && ln -s /opt/hadoop-3.1.3 /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

RUN mkdir -p /opt/airflow/config \
    && chown -R airflow: /opt/airflow/config

USER airflow

# Install dbt with DuckDB adapter
RUN pip install --no-cache-dir \
    "dbt-core==1.5.0" \
    "protobuf<3.21.0"

# Install dbt-duckdb and duckdb separately to avoid compilation issues
RUN pip install --no-deps dbt-duckdb==1.5.0 && \
    pip install --no-deps duckdb==0.8.1

# Create necessary directories for dbt
RUN mkdir -p /tmp/dbt_db && \
    chmod 777 /tmp/dbt_db && \
    mkdir -p /opt/airflow/dbt_logs && \
    chmod 777 /opt/airflow/dbt_logs

# # Copy and set up dbt initialization script
# COPY scripts/init_dbt.sh /opt/airflow/scripts/init_dbt.sh
# RUN chmod +x /opt/airflow/scripts/init_dbt.sh

# Set JAVA_HOME for Airflow user
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64