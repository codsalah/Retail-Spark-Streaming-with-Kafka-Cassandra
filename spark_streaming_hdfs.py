from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, expr, countDistinct, from_json, 
    hour, minute, dayofweek, date_format, sum, avg, 
    current_timestamp, round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, DoubleType, DateType
)

# Schema definitions
clickstream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("session_id", StringType(), True)
])

purchase_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
])

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RetailStreamingAnalytics") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

try:
    # Function to create streaming dataframe from Kafka
    def create_streaming_df(topic, schema):
        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka-iot:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load() \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

    # Process Clickstream data with enhanced metrics
    clickstream_df = create_streaming_df("clickstream", clickstream_schema)
    
    clickstream_enhanced = clickstream_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window("timestamp", "1 minute"),
            "page_url",
            "event_type"
        ) \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            hour("timestamp").alias("hour_of_day"),
            dayofweek("timestamp").alias("day_of_week")
        ) \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("engagement_score", 
            expr("CASE event_type " + 
                "WHEN 'purchase' THEN event_count * 5 " +
                "WHEN 'add_to_cart' THEN event_count * 3 " +
                "WHEN 'click' THEN event_count * 2 " +
                "ELSE event_count END"))

    # Process Purchase data
    purchase_df = create_streaming_df("purchases", purchase_schema)
    
    purchase_analytics = purchase_df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window("timestamp", "1 minute"),
            "product_id"
        ) \
        .agg(
            count("order_id").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            round(sum(col("quantity") * col("price")), 2).alias("total_revenue"),
            round(avg(col("price")), 2).alias("avg_price")
        ) \
        .withColumn("processing_time", current_timestamp())

    # Process Customer data
    customer_df = create_streaming_df("customers", customer_schema)
    
    # Process Customer data (without window/timestamp operations)
    customer_analytics = customer_df \
        .groupBy("country") \
        .agg(
            count("customer_id").alias("customer_count"),
            date_format(current_timestamp(), "yyyy-MM-dd").alias("snapshot_date")
        ) \
        .withColumn("processing_time", current_timestamp())

    # Write streams to HDFS silver layer
    def write_stream(df, path, checkpoint_path):
        return df.writeStream \
            .outputMode("append") \
            .format("csv") \
            .option("path", "hdfs://namenode:8020/silver/{}".format(path)) \
            .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/{}".format(checkpoint_path)) \
            .trigger(processingTime="1 minute") \
            .start()

    # Start all streaming queries
    queries = [
        write_stream(clickstream_enhanced, "clickstream_analytics", "clickstream"),
        write_stream(purchase_analytics, "purchase_analytics", "purchases"),
        write_stream(customer_analytics, "customer_analytics", "customers")
    ]

    # Wait for all queries to terminate
    for query in queries:
        query.awaitTermination()

except Exception as e:
    print("Error processing stream: {}".format(str(e)))
finally:
    spark.stop()
