DO $$
DECLARE
    v_customer_id BIGINT;
    v_order_id BIGINT;
BEGIN
    INSERT INTO retail.customers (
        first_name,
        last_name,
        email,
        phone,
        city,
        state,
        country
    )
    VALUES (
        'Sai',
        'Test',
        'sai.test@retailpulse.com',
        '555-0101',
        'Irving',
        'TX',
        'USA'
    )
    RETURNING customer_id INTO v_customer_id;

    INSERT INTO retail.orders (
        customer_id,
        order_status,
        order_total,
        order_channel
    )
    VALUES (
        v_customer_id,
        'COMPLETED',
        149.99,
        'ONLINE'
    )
    RETURNING order_id INTO v_order_id;

    INSERT INTO retail.order_items (
        order_id,
        product_id,
        quantity,
        unit_price
    )
    VALUES (
        v_order_id,
        1001,
        1,
        149.99
    );

    INSERT INTO retail.payments (
        order_id,
        payment_method,
        payment_status,
        amount,
        transaction_reference
    )
    VALUES (
        v_order_id,
        'CARD',
        'SUCCESS',
        149.99,
        'TXN-TEST-0001'
    );
END $$;