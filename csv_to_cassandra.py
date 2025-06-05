import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime

# Cassandra connection settings
CASSANDRA_HOST = 'localhost'
CASSANDRA_PORT = 9042
CASSANDRA_USER = 'cassandra'
CASSANDRA_PASS = 'cassandra'
KEYSPACE = 'retail'

# Connect to Cassandra
auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)

# Helper to parse ISO timestamps
parse_time = lambda t: datetime.fromisoformat(t) if t else None

# Insert clickstream data
with open('./data/clickstream.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        session.execute(
            """
            INSERT INTO clickstream (user_id, timestamp, page_url, event_type, product_id, session_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row['user_id'],
                parse_time(row['timestamp']),
                row['page_url'],
                row['event_type'],
                int(row['product_id']),
                row['session_id']
            )
        )

# Insert purchases data
with open('./data/purchases.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        session.execute(
            """
            INSERT INTO purchases (order_id, user_id, timestamp, product_id, quantity, price)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row['order_id'],
                row['user_id'],
                parse_time(row['timestamp']),
                int(row['product_id']),
                int(row['quantity']),
                float(row['price'])
            )
        )

# Insert customers data
with open('./data/customers.csv', newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        session.execute(
            """
            INSERT INTO customers (customer_id, name, email, country, signup_date)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                row['customer_id'],
                row['name'],
                row['email'],
                row['country'],
                parse_time(row['signup_date'])
            )
        )

## Print the first few rows of each table to verify data import
def print_tables_data():
    print("Clickstream Data:")
    rows = session.execute("SELECT * FROM clickstream LIMIT 3")
    for row in rows:
        print(row)

    print("\nPurchases Data:")
    rows = session.execute("SELECT * FROM purchases LIMIT 3")
    for row in rows:
        print(row)

    print("\nCustomers Data:")
    rows = session.execute("SELECT * FROM customers LIMIT 3")
    for row in rows:
        print(row)

print('CSV data imported into Cassandra.')


print_tables_data()

session.shutdown()
cluster.shutdown()
