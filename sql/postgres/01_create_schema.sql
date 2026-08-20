-- ============================================================
-- RetailPulse Enterprise Data Platform
-- Source System: Order Management System (PostgreSQL)
-- Purpose: Operational schema for retail transactions
-- ============================================================

CREATE SCHEMA IF NOT EXISTS retail;

-- ============================================================
-- CUSTOMERS
-- ============================================================

CREATE TABLE IF NOT EXISTS retail.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(30),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- ORDERS
-- ============================================================

CREATE TABLE IF NOT EXISTS retail.orders (
    order_id BIGSERIAL PRIMARY KEY,

    customer_id BIGINT NOT NULL
        REFERENCES retail.customers(customer_id),

    order_status VARCHAR(30) NOT NULL,

    order_total NUMERIC(12,2) NOT NULL
        CHECK (order_total >= 0),

    order_channel VARCHAR(30),

    order_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- ORDER ITEMS
-- ============================================================

CREATE TABLE IF NOT EXISTS retail.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,

    order_id BIGINT NOT NULL
        REFERENCES retail.orders(order_id),

    product_id BIGINT NOT NULL,

    quantity INTEGER NOT NULL
        CHECK (quantity > 0),

    unit_price NUMERIC(12,2) NOT NULL
        CHECK (unit_price >= 0),

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- PAYMENTS
-- ============================================================

CREATE TABLE IF NOT EXISTS retail.payments (
    payment_id BIGSERIAL PRIMARY KEY,

    order_id BIGINT NOT NULL
        REFERENCES retail.orders(order_id),

    payment_method VARCHAR(30) NOT NULL,

    payment_status VARCHAR(30) NOT NULL,

    amount NUMERIC(12,2) NOT NULL
        CHECK (amount >= 0),

    transaction_reference VARCHAR(100),

    payment_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON retail.orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_timestamp
    ON retail.orders(order_timestamp);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON retail.order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON retail.order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_payments_order_id
    ON retail.payments(order_id);