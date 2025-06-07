from faker import Faker
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import logging
import csv
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

fake = Faker()

# Initialize KafkaProducer with retry configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',  # Using the external port for local access
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,  # Number of retries if the connection fails
    retry_backoff_ms=1000,  # Time between retries in milliseconds
    security_protocol="PLAINTEXT"
)

def generate_clickstream():
    return {
        'user_id': fake.uuid4(),
        'timestamp': datetime.utcnow().isoformat(),
        'page_url': fake.url(),
        'event_type': fake.random_element(['page_view', 'click', 'scroll', 'add_to_cart', 'purchase']),
        'product_id': fake.random_int(min=1000, max=9999),
        'session_id': fake.uuid4()
    }

def generate_purchase():
    return {
        'order_id': fake.uuid4(),
        'user_id': fake.uuid4(),
        'timestamp': datetime.utcnow().isoformat(),
        'product_id': fake.random_int(min=1000, max=9999),
        'quantity': fake.random_int(min=1, max=5),
        'price': float(fake.random_number(digits=2)) / 100
    }

def generate_customer():
    return {
        'customer_id': fake.uuid4(),
        'name': fake.name(),
        'email': fake.email(),
        'country': fake.country(),
        'signup_date': fake.date_this_decade().isoformat()
    }

topics = {
    'clickstream': generate_clickstream,
    'purchases': generate_purchase,
    'customers': generate_customer
}

if __name__ == "__main__":
    logger.info("Starting Kafka producer...")
    # Initialize CSV files and writers if they don't exist or are empty
    csv_files = {topic: open(f"data/{topic}.csv", "a", newline="") for topic in topics}
    csv_writers = {}
    for topic, generator in topics.items():
        if os.stat(f"data/{topic}.csv").st_size == 0:
            sample = generator()
            writer = csv.DictWriter(csv_files[topic], fieldnames=sample.keys())
            writer.writeheader()
            csv_writers[topic] = writer
        else:
            sample = generator()
            writer = csv.DictWriter(csv_files[topic], fieldnames=sample.keys())
            csv_writers[topic] = writer
    # Ensure the data directory exists and is writable, create it if necessary
    try:
        while True:  # Continuous streaming
            for topic, generator in topics.items():
                record = generator()
                logger.info(f"Generated record for topic '{topic}': %s", record)
                try:
                    producer.send(topic, record)
                    logger.info(f"Sent message to topic '{topic}'")
                    # Write to CSV
                    csv_writers[topic].writerow(record)
                    csv_files[topic].flush()
                except Exception as e:
                    logger.error(f"Error sending message to topic '{topic}': %s", e)
            producer.flush()
            time.sleep(1)  # Simulate real-time streaming
    except KeyboardInterrupt:
        logger.info("Kafka producer stopped by user.")
    except Exception as e:
        logger.error("Fatal error: %s", e)
    finally:
        producer.close()
        for f in csv_files.values():
            f.close()
        logger.info("Kafka producer closed.")