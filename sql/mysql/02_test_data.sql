INSERT INTO suppliers (
    supplier_name,
    contact_name,
    email,
    phone,
    city,
    state,
    country
)
VALUES (
    'RetailPulse Test Supplier',
    'John Smith',
    'john.smith@supplier.com',
    '555-0201',
    'Dallas',
    'TX',
    'USA'
);

SET @supplier_id = LAST_INSERT_ID();


INSERT INTO products (
    product_id,
    supplier_id,
    product_name,
    category,
    brand,
    unit_cost,
    unit_price,
    product_status
)
VALUES (
    1001,
    @supplier_id,
    'Wireless Headphones',
    'Electronics',
    'RetailPulse Audio',
    75.00,
    149.99,
    'ACTIVE'
);


INSERT INTO stores (
    store_name,
    store_type,
    city,
    state,
    country,
    opened_date
)
VALUES (
    'RetailPulse Irving',
    'RETAIL',
    'Irving',
    'TX',
    'USA',
    '2025-01-15'
);

SET @store_id = LAST_INSERT_ID();


INSERT INTO inventory (
    product_id,
    store_id,
    quantity_on_hand,
    reorder_level,
    last_restocked_at
)
VALUES (
    1001,
    @store_id,
    75,
    20,
    CURRENT_TIMESTAMP
);