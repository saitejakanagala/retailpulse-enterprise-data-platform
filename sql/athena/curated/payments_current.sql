-- RetailPulse Enterprise Data Platform
-- Phase 6: Curated Current-State View
-- Table: payments

CREATE OR REPLACE VIEW retailpulse_curated.payments_current AS

WITH ranked_payments AS (
    SELECT
        payment_id,
        order_id,
        payment_method,
        payment_status,
        amount,
        transaction_reference,
        payment_timestamp,
        updated_at,
        standardized_at,
        load_date,

        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY
                COALESCE(updated_at, payment_timestamp) DESC,
                load_date DESC,
                standardized_at DESC
        ) AS rn

    FROM retailpulse_standardized.payments
)

SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    transaction_reference,
    payment_timestamp,
    updated_at,
    standardized_at,
    load_date
FROM ranked_payments
WHERE rn = 1;