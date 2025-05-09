CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS staging;


-- LOAD dim_date
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INT PRIMARY KEY,
    date DATE,
    day INT,
    day_name VARCHAR(20),
    month INT,
    month_name VARCHAR(20),
    year INT,
    week_of_year INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

COPY dw.dim_date
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';


-- LOAD dim_customers
CREATE TABLE IF NOT EXISTS dw.dim_customers (
    customer_id DOUBLE PRECISION PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(10),
    date_of_birth TIMESTAMP,
    email VARCHAR(255),
    location VARCHAR(255),
    signup_date TIMESTAMP,
    age INT,
    age_group VARCHAR(50),
    date_key INT REFERENCES dw.dim_date(date_key)  
);

COPY dw.dim_customers
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- LOAD dim_products
CREATE TABLE IF NOT EXISTS dw.dim_products (
    product_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    price DOUBLE PRECISION,
    rating DOUBLE PRECISION,
    created_at TIMESTAMP
);

COPY dw.dim_products
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- LOAD fact_orders
CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES dw.dim_customers(customer_id),
    order_date TIMESTAMP,
    order_status VARCHAR(50),
    total_amount DOUBLE PRECISION,
    net_amount DOUBLE PRECISION,
    date_key INT REFERENCES dw.dim_date(date_key)  
);

COPY dw.fact_orders
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';


-- LOAD fact_order_items
CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_id BIGINT REFERENCES dw.fact_orders(order_id),
    product_id BIGINT REFERENCES dw.dim_products(product_id),
    order_date TIMESTAMP,
    quantity BIGINT,
    price_each DOUBLE PRECISION,
    discount_percent DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    net_amount DOUBLE PRECISION,
    date_key INT REFERENCES dw.dim_date(date_key)  
);

COPY dw.fact_order_items
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';


-- LOAD fact_web_sessions
CREATE TABLE IF NOT EXISTS dw.fact_web_sessions (
    session_id BIGINT PRIMARY KEY,
    customer_id BIGINT REFERENCES dw.dim_customers(customer_id),
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    device_type VARCHAR(255),
    browser VARCHAR(255),
    date_key INT REFERENCES dw.dim_date(date_key)  
);

COPY dw.fact_web_sessions
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';



-- LOAD dim_campaigns
CREATE TABLE IF NOT EXISTS dw.dim_campaigns (
    campaign_id BIGINT PRIMARY KEY,
    product_id BIGINT,
    campaign_start_date TIMESTAMP,
    campaign_end_date TIMESTAMP,
    discount_percent BIGINT,
    date_key INT,
    FOREIGN KEY (product_id) REFERENCES dw.dim_products(product_id),
    FOREIGN KEY (date_key) REFERENCES dw.dim_date(date_key)
);

COPY dw.dim_campaigns
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- STAGING ad_clicks (since dont want to include click time in nanoseconds)
CREATE TABLE staging.temp_fact_ad_clicks (
    campaign_id BIGINT,
    click_id BIGINT,
    customer_id BIGINT,
    click_time BIGINT,
    ad_platform VARCHAR(255),
    click_timestamp TIMESTAMP,
    product_id BIGINT,
    discount_percent BIGINT,
    date_key INT
);

COPY staging.temp_fact_ad_clicks
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- LOAD fact_ad_clicks
CREATE TABLE IF NOT EXISTS dw.fact_ad_clicks (
    click_id BIGINT PRIMARY KEY,
    campaign_id BIGINT REFERENCES dw.dim_campaigns(campaign_id),
    customer_id BIGINT REFERENCES dw.dim_customers(customer_id),
    product_id BIGINT REFERENCES dw.dim_products(product_id),
    ad_platform VARCHAR(255),
    click_timestamp TIMESTAMP,
    discount_percent BIGINT,
    date_key INT REFERENCES dw.dim_date(date_key)  
);

INSERT INTO dw.fact_ad_clicks (
    click_id,
    campaign_id,
    customer_id,
    ad_platform,
    click_timestamp,
    product_id,
    discount_percent,
    date_key
)
SELECT
    click_id,
    campaign_id,
    customer_id,
    ad_platform,
    click_timestamp,
    product_id,
    discount_percent,
    date_key
FROM staging.temp_fact_ad_clicks;

-- LOAD fact_returns
CREATE TABLE dw.fact_returns (
    order_id BIGINT,
    product_id BIGINT,
    return_id BIGINT PRIMARY KEY,
    return_date TIMESTAMP,
    reason VARCHAR(255),
    refund_amount DOUBLE PRECISION,
    date_key INT,
    FOREIGN KEY (order_id) REFERENCES dw.fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dw.dim_products(product_id),
    FOREIGN KEY (date_key) REFERENCES dw.dim_date(date_key)
);

COPY dw.fact_returns
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- LOAD dim_shipments
CREATE TABLE dw.dim_shipments (
    shipment_id BIGINT PRIMARY KEY,
    order_id BIGINT,
    shipment_date TIMESTAMP,
    delivery_date TIMESTAMP,
    carrier TEXT,
    shipment_status TEXT,
    shipping_duration INT,
    is_delayed_delivery INT,
    FOREIGN KEY (order_id) REFERENCES dw.fact_orders(order_id)
);

COPY dw.dim_shipments
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';


-- LOAD dim_reviews
CREATE TABLE dw.dim_reviews (
    review_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    product_id BIGINT,
    review_date TIMESTAMP,
    rating BIGINT,
    review_text TEXT,
    sentiment VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES dw.dim_products(product_id)
);

COPY dw.dim_reviews
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- Staging inventory (and then using it to normalize the shipments table -> separated warehouse table)
CREATE TABLE staging.inventory (
    product_id BIGINT,
    warehouse VARCHAR(255),
    stock_quantity BIGINT,
    last_updated TIMESTAMP
);

COPY staging.inventory
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';

-- SELECT COUNT(*) FROM staging.inventory;

-- CREATING and LOADING warehouse table using staging.inventory
CREATE TABLE IF NOT EXISTS dw.dim_warehouse (
    warehouse_id INT PRIMARY KEY,
    warehouse_location VARCHAR(255) NOT NULL
);

-- DROP TABLE dw.fact_inventory;
-- DROP TABLE dw.dim_warehouse;

SELECT COUNT(*) FROM dw.dim_warehouse;

INSERT INTO dw.dim_warehouse (warehouse_id, warehouse_location)
SELECT 
    ROW_NUMBER() OVER (ORDER BY warehouse) AS warehouse_id,
    warehouse AS warehouse_location
FROM (SELECT DISTINCT warehouse FROM staging.inventory) AS unique_warehouses;


-- LOAD fact_inventory
CREATE TABLE IF NOT EXISTS dw.fact_inventory (
    product_id BIGINT,
    warehouse_id INT,
    stock_quantity BIGINT,
    last_updated TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES dw.dim_products (product_id),
    FOREIGN KEY (warehouse_id) REFERENCES dw.dim_warehouse (warehouse_id)
);

INSERT INTO dw.fact_inventory (product_id, warehouse_id, stock_quantity, last_updated)
SELECT 
    s.product_id,
    d.warehouse_id,
    s.stock_quantity,
    s.last_updated
FROM 
    staging.inventory s
JOIN 
    dw.dim_warehouse d
    ON s.warehouse = d.warehouse_location
JOIN 
    dw.dim_products p
    ON s.product_id = p.product_id;

-- SELECT COUNT(*) FROM dw.fact_inventory;

-- LOAD fact_cart_activity
CREATE TABLE IF NOT EXISTS dw.fact_cart_activity (
    activity_id BIGINT PRIMARY KEY,
    customer_id BIGINT,
    product_id BIGINT,
    activity_type VARCHAR(20),
    activity_time TIMESTAMP,
    date_key INT,
    FOREIGN KEY (customer_id) REFERENCES dw.dim_customers (customer_id),
    FOREIGN KEY (product_id) REFERENCES dw.dim_products (product_id),
    FOREIGN KEY (date_key) REFERENCES dw.dim_date (date_key)
);

COPY dw.fact_cart_activity
FROM '${S3_PATH}'
IAM_ROLE '${IAM_ROLE}'
FORMAT PARQUET
REGION '${REGION}';