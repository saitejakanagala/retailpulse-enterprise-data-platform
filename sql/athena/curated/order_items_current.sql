-- RetailPulse Enterprise Data Platform
-- Phase 6: Curated Current-State View
-- Table: order_items

CREATE OR REPLACE VIEW retailpulse_curated.order_items_current AS

WITH ranked_order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        created_at,
        updated_at,
        standardized_at,
        load_date,

        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY
                COALESCE(updated_at, created_at) DESC,
                load_date DESC,
                standardized_at DESC
        ) AS rn

    FROM retailpulse_standardized.order_items
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at,
    standardized_at,
    load_date
FROM ranked_order_items
WHERE rn = 1;