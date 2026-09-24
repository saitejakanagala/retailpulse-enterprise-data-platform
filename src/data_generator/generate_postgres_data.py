import os
import random
import uuid
from decimal import Decimal

import psycopg
from dotenv import load_dotenv
from faker import Faker


# ---------------------------------------------------------
# Environment
# ---------------------------------------------------------

load_dotenv()

DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# ---------------------------------------------------------
# Automated 12-hour batch configuration
# ---------------------------------------------------------

NEW_CUSTOMER_COUNT = 100

MIN_ORDERS_PER_CUSTOMER = 3
MAX_ORDERS_PER_CUSTOMER = 6

MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 3

CUSTOMER_UPDATE_COUNT = 15
ORDER_UPDATE_COUNT = 40
ORDER_ITEM_UPDATE_COUNT = 30
PAYMENT_UPDATE_COUNT = 25


ORDER_STATUSES = [
    "COMPLETED",
    "SHIPPED",
    "PENDING",
    "CANCELLED",
]

ORDER_CHANNELS = [
    "WEB",
    "MOBILE",
    "STORE",
]

PAYMENT_METHODS = [
    "CARD",
    "PAYPAL",
    "APPLE_PAY",
]

PAYMENT_STATUSES = [
    "SUCCESS",
    "FAILED",
]


fake = Faker()


# ---------------------------------------------------------
# Database connection
# ---------------------------------------------------------

def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ---------------------------------------------------------
# INSERT logic
# ---------------------------------------------------------

def create_customer(cursor):
    cursor.execute(
        """
        INSERT INTO retail.customers (
            first_name,
            last_name,
            email,
            phone,
            city,
            state,
            country
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING customer_id;
        """,
        (
            fake.first_name(),
            fake.last_name(),
            f"{uuid.uuid4().hex}@example.com",
            fake.phone_number()[:30],
            fake.city(),
            fake.state_abbr(),
            "USA",
        ),
    )

    return cursor.fetchone()[0]


def create_order(cursor, customer_id):
    order_status = random.choice(ORDER_STATUSES)
    order_channel = random.choice(ORDER_CHANNELS)

    cursor.execute(
        """
        INSERT INTO retail.orders (
            customer_id,
            order_status,
            order_total,
            order_channel
        )
        VALUES (%s, %s, %s, %s)
        RETURNING order_id;
        """,
        (
            customer_id,
            order_status,
            Decimal("0.00"),
            order_channel,
        ),
    )

    return cursor.fetchone()[0], order_status


def create_order_items(cursor, order_id):
    generated_count = random.randint(
        MIN_ITEMS_PER_ORDER,
        MAX_ITEMS_PER_ORDER,
    )

    order_total = Decimal("0.00")

    for _ in range(generated_count):
        product_id = random.randint(1001, 1050)
        quantity = random.randint(1, 4)

        unit_price = Decimal(
            str(round(random.uniform(5.99, 299.99), 2))
        )

        cursor.execute(
            """
            INSERT INTO retail.order_items (
                order_id,
                product_id,
                quantity,
                unit_price
            )
            VALUES (%s, %s, %s, %s);
            """,
            (
                order_id,
                product_id,
                quantity,
                unit_price,
            ),
        )

        order_total += unit_price * quantity

    cursor.execute(
        """
        UPDATE retail.orders
        SET order_total = %s
        WHERE order_id = %s;
        """,
        (
            order_total,
            order_id,
        ),
    )

    return order_total, generated_count


def create_payment(
    cursor,
    order_id,
    order_status,
    order_total,
):
    if order_status == "CANCELLED":
        return False

    payment_method = random.choice(PAYMENT_METHODS)

    if order_status == "PENDING":
        payment_status = random.choice(PAYMENT_STATUSES)
    else:
        payment_status = "SUCCESS"

    transaction_reference = (
        f"TXN-{uuid.uuid4().hex[:12].upper()}"
    )

    cursor.execute(
        """
        INSERT INTO retail.payments (
            order_id,
            payment_method,
            payment_status,
            amount,
            transaction_reference
        )
        VALUES (%s, %s, %s, %s, %s);
        """,
        (
            order_id,
            payment_method,
            payment_status,
            order_total,
            transaction_reference,
        ),
    )

    return True


# ---------------------------------------------------------
# UPDATE logic
# ---------------------------------------------------------

def update_existing_customers(cursor):
    cursor.execute(
        """
        SELECT customer_id
        FROM retail.customers
        ORDER BY RANDOM()
        LIMIT %s;
        """,
        (CUSTOMER_UPDATE_COUNT,),
    )

    customer_ids = [row[0] for row in cursor.fetchall()]

    for customer_id in customer_ids:
        cursor.execute(
            """
            UPDATE retail.customers
            SET
                phone = %s,
                city = %s,
                state = %s
            WHERE customer_id = %s;
            """,
            (
                fake.phone_number()[:30],
                fake.city(),
                fake.state_abbr(),
                customer_id,
            ),
        )

    return len(customer_ids)


def update_existing_orders(cursor):
    cursor.execute(
        """
        SELECT order_id
        FROM retail.orders
        ORDER BY RANDOM()
        LIMIT %s;
        """,
        (ORDER_UPDATE_COUNT,),
    )

    order_ids = [row[0] for row in cursor.fetchall()]

    for order_id in order_ids:
        cursor.execute(
            """
            UPDATE retail.orders
            SET order_status = %s
            WHERE order_id = %s;
            """,
            (
                random.choice(ORDER_STATUSES),
                order_id,
            ),
        )

    return len(order_ids)


def update_existing_order_items(cursor):
    cursor.execute(
        """
        SELECT order_item_id
        FROM retail.order_items
        ORDER BY RANDOM()
        LIMIT %s;
        """,
        (ORDER_ITEM_UPDATE_COUNT,),
    )

    order_item_ids = [row[0] for row in cursor.fetchall()]

    for order_item_id in order_item_ids:
        cursor.execute(
            """
            UPDATE retail.order_items
            SET quantity = %s
            WHERE order_item_id = %s;
            """,
            (
                random.randint(1, 4),
                order_item_id,
            ),
        )

    return len(order_item_ids)


def update_existing_payments(cursor):
    cursor.execute(
        """
        SELECT payment_id
        FROM retail.payments
        ORDER BY RANDOM()
        LIMIT %s;
        """,
        (PAYMENT_UPDATE_COUNT,),
    )

    payment_ids = [row[0] for row in cursor.fetchall()]

    for payment_id in payment_ids:
        cursor.execute(
            """
            UPDATE retail.payments
            SET payment_status = %s
            WHERE payment_id = %s;
            """,
            (
                random.choice(PAYMENT_STATUSES),
                payment_id,
            ),
        )

    return len(payment_ids)


# ---------------------------------------------------------
# Main batch generator
# ---------------------------------------------------------

def generate_data():
    customer_count = 0
    order_count = 0
    item_count = 0
    payment_count = 0

    with get_connection() as connection:
        with connection.cursor() as cursor:

            # ---------------------------------------------
            # Generate new transactional data
            # ---------------------------------------------

            for _ in range(NEW_CUSTOMER_COUNT):
                customer_id = create_customer(cursor)
                customer_count += 1

                orders_for_customer = random.randint(
                    MIN_ORDERS_PER_CUSTOMER,
                    MAX_ORDERS_PER_CUSTOMER,
                )

                for _ in range(orders_for_customer):
                    order_id, order_status = create_order(
                        cursor,
                        customer_id,
                    )

                    order_count += 1

                    order_total, generated_items = (
                        create_order_items(
                            cursor,
                            order_id,
                        )
                    )

                    item_count += generated_items

                    payment_created = create_payment(
                        cursor,
                        order_id,
                        order_status,
                        order_total,
                    )

                    if payment_created:
                        payment_count += 1

            # ---------------------------------------------
            # Generate realistic source updates
            # ---------------------------------------------

            customer_updates = update_existing_customers(cursor)
            order_updates = update_existing_orders(cursor)
            order_item_updates = update_existing_order_items(cursor)
            payment_updates = update_existing_payments(cursor)

        connection.commit()

    print()
    print("RetailPulse PostgreSQL automated batch completed.")
    print("------------------------------------------------")
    print()
    print("NEW RECORDS")
    print(f"Customers inserted : {customer_count}")
    print(f"Orders inserted    : {order_count}")
    print(f"Order items        : {item_count}")
    print(f"Payments inserted  : {payment_count}")
    print()
    print("UPDATED RECORDS")
    print(f"Customers updated  : {customer_updates}")
    print(f"Orders updated     : {order_updates}")
    print(f"Order items updated: {order_item_updates}")
    print(f"Payments updated   : {payment_updates}")


if __name__ == "__main__":
    generate_data()