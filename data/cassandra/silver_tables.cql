-- Create silver layer tables for retail analytics
USE retail;

-- Customers Silver Table
CREATE TABLE IF NOT EXISTS customers_silver (
    customer_id text PRIMARY KEY,
    name text,
    email text,
    country text,
    signup_date timestamp,
    processed_timestamp timestamp,
    signup_year int,
    signup_month int,
    signup_day int,
    email_domain text,
    data_source text
);

-- Purchases Silver Table
CREATE TABLE IF NOT EXISTS purchases_silver (
    order_id text PRIMARY KEY,
    user_id text,
    timestamp timestamp,
    product_id int,
    quantity int,
    unit_price decimal,
    total_amount decimal,
    processed_timestamp timestamp,
    purchase_date text,
    purchase_hour int,
    purchase_year int,
    purchase_month int,
    purchase_day int,
    data_source text
);

-- Clickstream Silver Table
CREATE TABLE IF NOT EXISTS clickstream_silver (
    user_id text,
    timestamp timestamp,
    page_url text,
    event_type text,
    product_id int,
    session_id text,
    processed_timestamp timestamp,
    event_date text,
    event_hour int,
    event_year int,
    event_month int,
    event_day int,
    data_source text,
    PRIMARY KEY (user_id, timestamp)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_customers_email_domain ON customers_silver (email_domain);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers_silver (country);
CREATE INDEX IF NOT EXISTS idx_customers_signup_year ON customers_silver (signup_year);

CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases_silver (user_id);
CREATE INDEX IF NOT EXISTS idx_purchases_product_id ON purchases_silver (product_id);
CREATE INDEX IF NOT EXISTS idx_purchases_year ON purchases_silver (purchase_year);
CREATE INDEX IF NOT EXISTS idx_purchases_month ON purchases_silver (purchase_month);

CREATE INDEX IF NOT EXISTS idx_clickstream_event_type ON clickstream_silver (event_type);
CREATE INDEX IF NOT EXISTS idx_clickstream_product_id ON clickstream_silver (product_id);
CREATE INDEX IF NOT EXISTS idx_clickstream_session_id ON clickstream_silver (session_id);
CREATE INDEX IF NOT EXISTS idx_clickstream_year ON clickstream_silver (event_year);
CREATE INDEX IF NOT EXISTS idx_clickstream_month ON clickstream_silver (event_month);
