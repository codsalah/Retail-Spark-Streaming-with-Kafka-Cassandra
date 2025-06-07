#!/bin/bash

# Copy the script to spark master
docker cp spark_streaming_hdfs.py spark-master:/opt/spark/apps/
docker cp spark-jars/spark-cassandra-connector_2.12-3.3.0.jar spark-master:/opt/spark-jars/

# Submit the Spark job
docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark-jars/spark-cassandra-connector_2.12-3.3.0.jar \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    /opt/spark/apps/spark_streaming_hdfs.py
