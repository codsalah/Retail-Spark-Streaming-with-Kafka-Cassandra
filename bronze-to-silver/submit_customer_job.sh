#!/bin/bash

# Set Spark home and other environment variables
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf

# Submit the Spark job with necessary configurations and dependencies
spark-submit \
  --master local[*] \
  --name "Customer Bronze to Silver" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,org.slf4j:slf4j-api:1.7.32,org.apache.commons:commons-lang3:3.10 \
  --conf spark.cassandra.connection.host=cassandra \
  --conf spark.cassandra.connection.port=9042 \
  --conf spark.cassandra.auth.username=cassandra \
  --conf spark.cassandra.auth.password=cassandra \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
  spark_cassandra_cust.py
