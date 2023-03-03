CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(128) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_usd NUMERIC(12, 2) NOT NULL,
    total_price_usd NUMERIC(12, 2) NOT NULL,
    order_ts TIMESTAMPTZ NOT NULL,
    status VARCHAR(32) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON fact_orders (status);
