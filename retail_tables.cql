-- Keyspace
CREATE KEYSPACE IF NOT EXISTS retail WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Clickstream Table
CREATE TABLE IF NOT EXISTS retail.clickstream (
    user_id text,
    timestamp timestamp,
    page_url text,
    event_type text,
    product_id int,
    session_id text,
    PRIMARY KEY (user_id, timestamp)
);

-- Purchases Table
CREATE TABLE IF NOT EXISTS retail.purchases (
    order_id text PRIMARY KEY,
    user_id text,
    timestamp timestamp,
    product_id int,
    quantity int,
    price double
);

-- Customers Table
CREATE TABLE IF NOT EXISTS retail.customers (
    customer_id text PRIMARY KEY,
    name text,
    email text,
    country text,
    signup_date timestamp
);
