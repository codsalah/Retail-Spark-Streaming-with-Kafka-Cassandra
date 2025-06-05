FROM apache/airflow:2.5.1

USER root

# Install Hadoop client
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download and setup Hadoop client
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.1.3/hadoop-3.1.3.tar.gz \
    && tar -xzf hadoop-3.1.3.tar.gz -C /opt \
    && rm hadoop-3.1.3.tar.gz \
    && ln -s /opt/hadoop-3.1.3 /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

RUN mkdir -p /opt/airflow/config \
    && chown -R airflow: /opt/airflow/config

USER airflow

RUN pip install --no-cache-dir \
    "dbt-core" \
    "protobuf<3.21.0"

# Set JAVA_HOME for Airflow user
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64