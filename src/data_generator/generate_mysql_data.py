import os
import random

from dotenv import load_dotenv
from faker import Faker
import mysql.connector


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------

load_dotenv()

DB_HOST = "127.0.0.1"
DB_PORT = int(os.getenv("MYSQL_PORT", "3307"))
DB_NAME = os.getenv("MYSQL_DATABASE")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")


# ---------------------------------------------------------
# Generator configuration
# ---------------------------------------------------------

SUPPLIER_COUNT = 10
STORE_COUNT = 10

PRODUCT_START_ID = 1001
PRODUCT_END_ID = 1050

MIN_STOCK = 0
MAX_STOCK = 300

MIN_REORDER_LEVEL = 10
MAX_REORDER_LEVEL = 50


PRODUCT_CATEGORIES = [
    "Electronics",
    "Home",
    "Kitchen",
    "Clothing",
    "Sports",
    "Beauty",
]

STORE_TYPES = [
    "RETAIL",
    "OUTLET",
    "FULFILLMENT",
]

PRODUCT_STATUSES = [
    "ACTIVE",
    "INACTIVE",
]

fake = Faker()


# ---------------------------------------------------------
# Database connection
# ---------------------------------------------------------

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ---------------------------------------------------------
# Create suppliers
# ---------------------------------------------------------

def create_suppliers(cursor):
    supplier_ids = []

    for _ in range(SUPPLIER_COUNT):
        cursor.execute(
            """
            INSERT INTO suppliers (
                supplier_name,
                contact_name,
                email,
                phone,
                city,
                state,
                country
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                fake.company(),
                fake.name(),
                fake.unique.company_email(),
                fake.phone_number()[:30],
                fake.city(),
                fake.state_abbr(),
                "USA",
            ),
        )

        supplier_ids.append(cursor.lastrowid)

    return supplier_ids


# ---------------------------------------------------------
# Create stores
# ---------------------------------------------------------

def create_stores(cursor):
    store_ids = []

    for _ in range(STORE_COUNT):
        cursor.execute(
            """
            INSERT INTO stores (
                store_name,
                store_type,
                city,
                state,
                country,
                opened_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                f"RetailPulse {fake.city()}",
                random.choice(STORE_TYPES),
                fake.city(),
                fake.state_abbr(),
                "USA",
                fake.date_between(
                    start_date="-10y",
                    end_date="today"
                ),
            ),
        )

        store_ids.append(cursor.lastrowid)

    return store_ids


# ---------------------------------------------------------
# Create products
# ---------------------------------------------------------

def create_products(cursor, supplier_ids):
    product_ids = []

    # Find the highest product_id already in the database
    cursor.execute(
        "SELECT COALESCE(MAX(product_id), %s) FROM products",
        (PRODUCT_START_ID - 1,)
    )

    max_product_id = cursor.fetchone()[0]

    # Start new product IDs after the current maximum
    start_product_id = max_product_id + 1

    # Generate the same number of products as before
    product_count = PRODUCT_END_ID - PRODUCT_START_ID + 1

    end_product_id = start_product_id + product_count

    print(
        f"Generating products from "
        f"{start_product_id} to {end_product_id - 1}"
    )

    for product_id in range(
        start_product_id,
        end_product_id,
    ):
        supplier_id = random.choice(supplier_ids)

        unit_cost = round(
            random.uniform(5.00, 200.00),
            2,
        )

        unit_price = round(
            unit_cost * random.uniform(1.20, 2.00),
            2,
        )

        category = random.choice(PRODUCT_CATEGORIES)

        cursor.execute(
            """
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                product_id,
                supplier_id,
                fake.catch_phrase(),
                category,
                fake.company(),
                unit_cost,
                unit_price,
                random.choice(PRODUCT_STATUSES),
            ),
        )

        product_ids.append(product_id)

    return product_ids


# ---------------------------------------------------------
# Create inventory
# ---------------------------------------------------------

def create_inventory(
    cursor,
    product_ids,
    store_ids,
):
    inventory_count = 0

    for product_id in product_ids:
        for store_id in store_ids:

            quantity_on_hand = random.randint(
                MIN_STOCK,
                MAX_STOCK,
            )

            reorder_level = random.randint(
                MIN_REORDER_LEVEL,
                MAX_REORDER_LEVEL,
            )

            cursor.execute(
                """
                INSERT INTO inventory (
                    product_id,
                    store_id,
                    quantity_on_hand,
                    reorder_level,
                    last_restocked_at
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    product_id,
                    store_id,
                    quantity_on_hand,
                    reorder_level,
                    fake.date_time_between(
                        start_date="-180d",
                        end_date="now"
                    ),
                ),
            )

            inventory_count += 1

    return inventory_count


# ---------------------------------------------------------
# Main generator
# ---------------------------------------------------------

def generate_data():
    connection = get_connection()
    cursor = connection.cursor()

    try:
        supplier_ids = create_suppliers(cursor)

        store_ids = create_stores(cursor)

        product_ids = create_products(
            cursor,
            supplier_ids,
        )

        inventory_count = create_inventory(
            cursor,
            product_ids,
            store_ids,
        )

        connection.commit()

        print()
        print("RetailPulse MySQL data generation completed.")
        print("-----------------------------------------")
        print(f"Suppliers inserted : {len(supplier_ids)}")
        print(f"Stores inserted    : {len(store_ids)}")
        print(f"Products inserted  : {len(product_ids)}")
        print(f"Inventory rows     : {inventory_count}")

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    generate_data()