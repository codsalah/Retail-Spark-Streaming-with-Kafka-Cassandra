from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, date_format, upper, lower, trim,
    round, expr, split, when, isnan, isnull, regexp_replace, 
    year, month, dayofmonth, hour, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType
)
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("CSVToSilverBatch") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def ensure_silver_directory():
    """Ensure the silver directory exists"""
    silver_dir = "/opt/spark/data/silver"
    if not os.path.exists(silver_dir):
        os.makedirs(silver_dir)
        logger.info("Created silver directory at {}".format(silver_dir))

def process_clickstream_data(spark):
    """Process clickstream data with quality improvements"""
    logger.info("Processing clickstream data...")
    
    # Read clickstream CSV
    clickstream_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/opt/spark/data/clickstream.csv")
    
    # Data transformations and quality improvements
    clickstream_silver = clickstream_df \
        .withColumn("user_id", trim(col("user_id"))) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("page_url", trim(col("page_url"))) \
        .withColumn("event_type", upper(trim(col("event_type")))) \
        .withColumn("session_id", trim(col("session_id"))) \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("event_date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .withColumn("event_hour", hour(col("timestamp"))) \
        .withColumn("event_year", year(col("timestamp"))) \
        .withColumn("event_month", month(col("timestamp"))) \
        .withColumn("event_day", dayofmonth(col("timestamp"))) \
        .withColumn("data_source", lit("batch_csv")) \
        .withColumn("record_status", 
            when(col("user_id").isNull() | (col("user_id") == ""), "INVALID")
            .when(col("timestamp").isNull(), "INVALID")
            .when(col("event_type").isNull() | (col("event_type") == ""), "INVALID")
            .otherwise("VALID")) \
        .filter(col("record_status") == "VALID") \
        .select(
            "user_id", "timestamp", "page_url", "event_type", "product_id", 
            "session_id", "processed_timestamp", "event_date", "event_hour",
            "event_year", "event_month", "event_day", "data_source"
        )
    
    return clickstream_silver

def process_purchases_data(spark):
    """Process purchases data with quality improvements"""
    logger.info("Processing purchases data...")
    
    # Read purchases CSV
    purchases_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/opt/spark/data/purchases.csv")
    
    # Data transformations and quality improvements
    purchases_silver = purchases_df \
        .withColumn("order_id", trim(col("order_id"))) \
        .withColumn("user_id", trim(col("user_id"))) \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("purchase_date", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .withColumn("purchase_hour", hour(col("timestamp"))) \
        .withColumn("purchase_year", year(col("timestamp"))) \
        .withColumn("purchase_month", month(col("timestamp"))) \
        .withColumn("purchase_day", dayofmonth(col("timestamp"))) \
        .withColumn("total_amount", round(col("quantity") * col("price"), 2)) \
        .withColumn("unit_price", round(col("price"), 2)) \
        .withColumn("data_source", lit("batch_csv")) \
        .withColumn("record_status", 
            when(col("order_id").isNull() | (col("order_id") == ""), "INVALID")
            .when(col("user_id").isNull() | (col("user_id") == ""), "INVALID")
            .when(col("timestamp").isNull(), "INVALID")
            .when(col("quantity") <= 0, "INVALID")
            .when(col("price") < 0, "INVALID")
            .otherwise("VALID")) \
        .filter(col("record_status") == "VALID") \
        .select(
            "order_id", "user_id", "timestamp", "product_id", "quantity", 
            "unit_price", "total_amount", "processed_timestamp", "purchase_date", 
            "purchase_hour", "purchase_year", "purchase_month", "purchase_day", 
            "data_source"
        )
    
    return purchases_silver

def process_customers_data(spark):
    """Process customers data with quality improvements"""
    logger.info("Processing customers data...")
    
    # Read customers CSV
    customers_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/opt/spark/data/customers.csv")
    
    # Data transformations and quality improvements
    customers_silver = customers_df \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("name", trim(col("name"))) \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("country", upper(trim(col("country")))) \
        .withColumn("signup_date", to_timestamp(col("signup_date"))) \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("signup_year", year(col("signup_date"))) \
        .withColumn("signup_month", month(col("signup_date"))) \
        .withColumn("signup_day", dayofmonth(col("signup_date"))) \
        .withColumn("data_source", lit("batch_csv")) \
        .withColumn("email_domain", 
            expr("substring_index(email, '@', -1)")) \
        .withColumn("record_status", 
            when(col("customer_id").isNull() | (col("customer_id") == ""), "INVALID")
            .when(col("name").isNull() | (col("name") == ""), "INVALID")
            .when(col("email").isNull() | (col("email") == ""), "INVALID")
            .when(~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,6}$"), "INVALID")
            .otherwise("VALID")) \
        .filter(col("record_status") == "VALID") \
        .select(
            "customer_id", "name", "email", "country", "signup_date", 
            "processed_timestamp", "signup_year", "signup_month", "signup_day",
            "email_domain", "data_source"
        )
    
    return customers_silver

def write_to_csv(df, table_name):
    """Write DataFrame to CSV in silver directory"""
    output_path = "/opt/spark/data/silver/{}".format(table_name)
    logger.info("Writing {} data to {}".format(table_name, output_path))

    # Write as single CSV file with header
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    logger.info("Successfully wrote {} records to {}".format(df.count(), output_path))

def print_data_quality_summary(df, table_name):
    """Print data quality summary"""
    total_records = df.count()
    logger.info("\n=== {} DATA QUALITY SUMMARY ===".format(table_name.upper()))
    logger.info("Total records processed: {}".format(total_records))

    # Show sample data
    logger.info("Sample {} data:".format(table_name))
    df.show(5, truncate=False)

    # Show schema
    logger.info("{} schema:".format(table_name))
    df.printSchema()

def main():
    """Main processing function"""
    logger.info("Starting CSV to Silver batch processing...")
    
    # Ensure silver directory exists
    ensure_silver_directory()
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process each dataset
        clickstream_silver = process_clickstream_data(spark)
        purchases_silver = process_purchases_data(spark)
        customers_silver = process_customers_data(spark)
        
        # Print data quality summaries
        print_data_quality_summary(clickstream_silver, "clickstream")
        print_data_quality_summary(purchases_silver, "purchases")
        print_data_quality_summary(customers_silver, "customers")
        
        # Write to CSV files
        write_to_csv(clickstream_silver, "clickstream")
        write_to_csv(purchases_silver, "purchases")
        write_to_csv(customers_silver, "customers")
        
        logger.info("Successfully completed CSV to Silver processing!")
        
    except Exception as e:
        logger.error("Error in processing: {}".format(str(e)), exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
