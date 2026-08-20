import os
import random
import uuid
from decimal import Decimal

import psycopg
from dotenv import load_dotenv
from faker import Faker


# Load values from .env
load_dotenv()

DB_HOST = "127.0.0.1"
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


# Small test batch first
CUSTOMER_COUNT = 1000

MIN_ORDERS_PER_CUSTOMER = 1
MAX_ORDERS_PER_CUSTOMER = 3

MIN_ITEMS_PER_ORDER = 1
MAX_ITEMS_PER_ORDER = 3


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


fake = Faker()


def get_connection():
    return psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


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

    order_id = cursor.fetchone()[0]

    return order_id, order_status


def create_order_items(cursor, order_id):
    item_count = random.randint(
        MIN_ITEMS_PER_ORDER,
        MAX_ITEMS_PER_ORDER,
    )

    order_total = Decimal("0.00")

    for _ in range(item_count):
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
        SET
            order_total = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE order_id = %s;
        """,
        (
            order_total,
            order_id,
        ),
    )

    return order_total, item_count


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
        payment_status = random.choice(
            ["SUCCESS", "FAILED"]
        )
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


def generate_data():
    customer_count = 0
    order_count = 0
    item_count = 0
    payment_count = 0

    with get_connection() as connection:
        with connection.cursor() as cursor:

            for _ in range(CUSTOMER_COUNT):
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

                    order_total, generated_items = create_order_items(
                        cursor,
                        order_id,
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

        connection.commit()

    print()
    print("RetailPulse PostgreSQL data generation completed.")
    print("---------------------------------------------")
    print(f"Customers inserted : {customer_count}")
    print(f"Orders inserted    : {order_count}")
    print(f"Order items        : {item_count}")
    print(f"Payments inserted  : {payment_count}")


if __name__ == "__main__":
    generate_data()