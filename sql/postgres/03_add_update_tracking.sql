-- ============================================================
-- RetailPulse Enterprise Data Platform
-- PostgreSQL Update Tracking Migration
-- Purpose:
--   Enable INSERT + UPDATE incremental ingestion
--   using (updated_at, primary_key) watermarks
-- ============================================================

BEGIN;


-- ============================================================
-- ORDER ITEMS
-- Add updated_at and preserve historical timestamps
-- ============================================================

ALTER TABLE retail.order_items
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

UPDATE retail.order_items
SET updated_at = created_at
WHERE updated_at IS NULL;

ALTER TABLE retail.order_items
ALTER COLUMN updated_at
SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE retail.order_items
ALTER COLUMN updated_at
SET NOT NULL;


-- ============================================================
-- PAYMENTS
-- Add updated_at and preserve historical timestamps
-- ============================================================

ALTER TABLE retail.payments
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

UPDATE retail.payments
SET updated_at = payment_timestamp
WHERE updated_at IS NULL;

ALTER TABLE retail.payments
ALTER COLUMN updated_at
SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE retail.payments
ALTER COLUMN updated_at
SET NOT NULL;


-- ============================================================
-- Shared trigger function
-- Automatically refresh updated_at whenever a row changes
-- ============================================================

CREATE OR REPLACE FUNCTION retail.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


-- ============================================================
-- CUSTOMERS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_customers_updated_at
ON retail.customers;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE
ON retail.customers
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- ORDERS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_orders_updated_at
ON retail.orders;

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE
ON retail.orders
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- ORDER ITEMS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_order_items_updated_at
ON retail.order_items;

CREATE TRIGGER trg_order_items_updated_at
BEFORE UPDATE
ON retail.order_items
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- PAYMENTS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_payments_updated_at
ON retail.payments;

CREATE TRIGGER trg_payments_updated_at
BEFORE UPDATE
ON retail.payments
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- Incremental ingestion indexes
-- Improve composite watermark query performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_customers_updated_at_id
    ON retail.customers(updated_at, customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at_id
    ON retail.orders(updated_at, order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_updated_at_id
    ON retail.order_items(updated_at, order_item_id);

CREATE INDEX IF NOT EXISTS idx_payments_updated_at_id
    ON retail.payments(updated_at, payment_id);


COMMIT;