from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ------------------- Spark Session Configuration -------------------
# Initialize Spark session with Cassandra connector configuration
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra-iot") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.jars", "/opt/spark-jars/spark-cassandra-connector_2.12-3.3.0.jar") \
    .getOrCreate()

# ------------------- Schema Definitions -------------------
clickstream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("session_id", StringType(), True)
])

purchase_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", StringType(), True)
])

# ------------------- Kafka Stream Definitions -------------------
# Configure Kafka source streams for each topic (clickstream, purchases, customers)
# with earliest offset to process all available messages

kafka_df_clicks = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-iot:29092") \
    .option("subscribe", "clickstream") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df_purchases = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-iot:29092") \
    .option("subscribe", "purchases") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df_customers = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-iot:29092") \
    .option("subscribe", "customers") \
    .option("startingOffsets", "earliest") \
    .load()
# ------------------- Data Processing -------------------
# Parse JSON data from Kafka messages and convert timestamps to proper format

clickstream_df = kafka_df_clicks.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), clickstream_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

purchase_df = kafka_df_purchases.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), purchase_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

customer_df = kafka_df_customers.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), customer_schema).alias("data")) \
    .select("data.*") \
    .withColumn("signup_date", to_timestamp("signup_date"))


# ------------------- Bronze Layer Storage -------------------
# Write streaming data to HDFS (Parquet format) and Cassandra
# Data is processed in 4-minute intervals for both destinations

# Writing to HDFS Bronze Layer
clickstream_bronze = clickstream_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/bronze/clickstream") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/clickstream_bronze") \
    .trigger(processingTime="4 minutes") \
    .start()

purchase_bronze = purchase_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/bronze/purchases") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/purchases_bronze") \
    .trigger(processingTime="4 minutes") \
    .start()

customer_bronze = customer_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/bronze/customers") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/customers_bronze") \
    .trigger(processingTime="30 seconds") \
    .start()

# Writing to Cassandra Real-time Layer
clickstream_cassandra = clickstream_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/clickstream_cassandra") \
    .option("keyspace", "retail") \
    .option("table", "clickstream") \
    .start()

purchase_cassandra = purchase_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/purchases_cassandra") \
    .option("keyspace", "retail") \
    .option("table", "purchases") \
    .start()

customer_cassandra = customer_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/customers_cassandra") \
    .option("keyspace", "retail") \
    .option("table", "customers") \
    .start()

# ------------------- Silver Layer Processing -------------------
# Transform data from Bronze layer and write to Silver layer

# Clickstream Silver Layer
clickstream_silver = clickstream_df.filter(col("event_type").isNotNull()) \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/silver/clickstream") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/clickstream_silver") \
    .trigger(processingTime="4 minutes") \
    .start()

# Purchases Silver Layer
purchase_silver = purchase_df.filter(col("quantity") > 0) \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/silver/purchases") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/purchases_silver") \
    .trigger(processingTime="30 seconds") \
    .start()

# Customers Silver Layer
customer_silver = customer_df.filter(col("email").isNotNull()) \
    .writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:8020/silver/customers") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/customers_silver") \
    .trigger(processingTime="30 seconds") \
    .start()

# Wait for the streaming queries to terminate
spark.streams.awaitAnyTermination()