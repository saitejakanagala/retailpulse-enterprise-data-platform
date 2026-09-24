import os
import random
import uuid

from dotenv import load_dotenv
from faker import Faker
import mysql.connector


# ---------------------------------------------------------
# Environment
# ---------------------------------------------------------

load_dotenv()

DB_HOST = "127.0.0.1"
DB_PORT = int(os.getenv("MYSQL_PORT", "3307"))
DB_NAME = os.getenv("MYSQL_DATABASE")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")


# ---------------------------------------------------------
# Automated 12-hour batch configuration
# ---------------------------------------------------------

# Slowly changing master data
NEW_SUPPLIER_COUNT = 1
NEW_STORE_COUNT = 0

# Product/catalog activity
NEW_PRODUCT_COUNT = 20

# Create inventory for each new product at a few stores
INVENTORY_STORES_PER_NEW_PRODUCT = 5

# Existing-row updates each batch
SUPPLIER_UPDATE_COUNT = 3
STORE_UPDATE_COUNT = 2
PRODUCT_UPDATE_COUNT = 15
INVENTORY_UPDATE_COUNT = 200


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
# Existing ID helpers
# ---------------------------------------------------------

def get_supplier_ids(cursor):
    cursor.execute(
        """
        SELECT supplier_id
        FROM suppliers
        ORDER BY supplier_id;
        """
    )

    return [row[0] for row in cursor.fetchall()]


def get_store_ids(cursor):
    cursor.execute(
        """
        SELECT store_id
        FROM stores
        ORDER BY store_id;
        """
    )

    return [row[0] for row in cursor.fetchall()]


# ---------------------------------------------------------
# UPDATE existing source records
# ---------------------------------------------------------

def update_existing_suppliers(cursor):
    cursor.execute(
        """
        SELECT supplier_id
        FROM suppliers
        ORDER BY RAND()
        LIMIT %s;
        """,
        (SUPPLIER_UPDATE_COUNT,),
    )

    supplier_ids = [row[0] for row in cursor.fetchall()]

    for supplier_id in supplier_ids:
        cursor.execute(
            """
            UPDATE suppliers
            SET
                contact_name = %s,
                phone = %s,
                city = %s,
                state = %s
            WHERE supplier_id = %s;
            """,
            (
                fake.name(),
                fake.phone_number()[:30],
                fake.city(),
                fake.state_abbr(),
                supplier_id,
            ),
        )

    return len(supplier_ids)


def update_existing_stores(cursor):
    cursor.execute(
        """
        SELECT store_id
        FROM stores
        ORDER BY RAND()
        LIMIT %s;
        """,
        (STORE_UPDATE_COUNT,),
    )

    store_ids = [row[0] for row in cursor.fetchall()]

    for store_id in store_ids:
        cursor.execute(
            """
            UPDATE stores
            SET
                store_type = %s,
                city = %s,
                state = %s
            WHERE store_id = %s;
            """,
            (
                random.choice(STORE_TYPES),
                fake.city(),
                fake.state_abbr(),
                store_id,
            ),
        )

    return len(store_ids)


def update_existing_products(cursor):
    cursor.execute(
        """
        SELECT
            product_id,
            unit_cost
        FROM products
        ORDER BY RAND()
        LIMIT %s;
        """,
        (PRODUCT_UPDATE_COUNT,),
    )

    products = cursor.fetchall()

    for product_id, unit_cost in products:
        multiplier = random.uniform(1.20, 2.00)

        new_price = round(
            float(unit_cost) * multiplier,
            2,
        )

        cursor.execute(
            """
            UPDATE products
            SET
                unit_price = %s,
                product_status = %s
            WHERE product_id = %s;
            """,
            (
                new_price,
                random.choice(PRODUCT_STATUSES),
                product_id,
            ),
        )

    return len(products)


def update_existing_inventory(cursor):
    cursor.execute(
        """
        SELECT inventory_id
        FROM inventory
        ORDER BY RAND()
        LIMIT %s;
        """,
        (INVENTORY_UPDATE_COUNT,),
    )

    inventory_ids = [
        row[0]
        for row in cursor.fetchall()
    ]

    for inventory_id in inventory_ids:
        cursor.execute(
            """
            UPDATE inventory
            SET
                quantity_on_hand = %s,
                reorder_level = %s,
                last_restocked_at = CURRENT_TIMESTAMP
            WHERE inventory_id = %s;
            """,
            (
                random.randint(
                    MIN_STOCK,
                    MAX_STOCK,
                ),
                random.randint(
                    MIN_REORDER_LEVEL,
                    MAX_REORDER_LEVEL,
                ),
                inventory_id,
            ),
        )

    return len(inventory_ids)


# ---------------------------------------------------------
# INSERT new suppliers
# ---------------------------------------------------------

def create_suppliers(cursor):
    supplier_ids = []

    for _ in range(NEW_SUPPLIER_COUNT):
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
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            (
                fake.company(),
                fake.name(),
                f"{uuid.uuid4().hex}@supplier.example.com",
                fake.phone_number()[:30],
                fake.city(),
                fake.state_abbr(),
                "USA",
            ),
        )

        supplier_ids.append(
            cursor.lastrowid
        )

    return supplier_ids


# ---------------------------------------------------------
# INSERT new stores
# ---------------------------------------------------------

def create_stores(cursor):
    store_ids = []

    for _ in range(NEW_STORE_COUNT):
        city = fake.city()

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
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                f"RetailPulse {city}",
                random.choice(STORE_TYPES),
                city,
                fake.state_abbr(),
                "USA",
                fake.date_between(
                    start_date="-10y",
                    end_date="today",
                ),
            ),
        )

        store_ids.append(
            cursor.lastrowid
        )

    return store_ids


# ---------------------------------------------------------
# INSERT new products
# ---------------------------------------------------------

def create_products(
    cursor,
    supplier_ids,
):
    if not supplier_ids:
        raise RuntimeError(
            "No suppliers available for product generation."
        )

    cursor.execute(
        """
        SELECT COALESCE(MAX(product_id), 1000)
        FROM products;
        """
    )

    max_product_id = cursor.fetchone()[0]

    start_product_id = (
        max_product_id + 1
    )

    end_product_id = (
        start_product_id
        + NEW_PRODUCT_COUNT
    )

    product_ids = []

    print(
        f"Generating products from "
        f"{start_product_id} "
        f"to {end_product_id - 1}"
    )

    for product_id in range(
        start_product_id,
        end_product_id,
    ):
        supplier_id = random.choice(
            supplier_ids
        )

        unit_cost = round(
            random.uniform(
                5.00,
                200.00,
            ),
            2,
        )

        unit_price = round(
            unit_cost
            * random.uniform(
                1.20,
                2.00,
            ),
            2,
        )

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
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            );
            """,
            (
                product_id,
                supplier_id,
                fake.catch_phrase(),
                random.choice(
                    PRODUCT_CATEGORIES
                ),
                fake.company(),
                unit_cost,
                unit_price,
                random.choice(
                    PRODUCT_STATUSES
                ),
            ),
        )

        product_ids.append(
            product_id
        )

    return product_ids


# ---------------------------------------------------------
# INSERT inventory for new products
# ---------------------------------------------------------

def create_inventory_for_new_products(
    cursor,
    product_ids,
    store_ids,
):
    if not store_ids:
        raise RuntimeError(
            "No stores available for inventory generation."
        )

    inventory_count = 0

    stores_per_product = min(
        INVENTORY_STORES_PER_NEW_PRODUCT,
        len(store_ids),
    )

    for product_id in product_ids:

        selected_store_ids = random.sample(
            store_ids,
            stores_per_product,
        )

        for store_id in selected_store_ids:

            cursor.execute(
                """
                INSERT INTO inventory (
                    product_id,
                    store_id,
                    quantity_on_hand,
                    reorder_level,
                    last_restocked_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    CURRENT_TIMESTAMP
                );
                """,
                (
                    product_id,
                    store_id,
                    random.randint(
                        MIN_STOCK,
                        MAX_STOCK,
                    ),
                    random.randint(
                        MIN_REORDER_LEVEL,
                        MAX_REORDER_LEVEL,
                    ),
                ),
            )

            inventory_count += 1

    return inventory_count


# ---------------------------------------------------------
# Main automated batch
# ---------------------------------------------------------

def generate_data():
    connection = get_connection()
    cursor = connection.cursor()

    try:

        # -------------------------------------------------
        # Update existing business records
        # -------------------------------------------------

        supplier_updates = (
            update_existing_suppliers(
                cursor
            )
        )

        store_updates = (
            update_existing_stores(
                cursor
            )
        )

        product_updates = (
            update_existing_products(
                cursor
            )
        )

        inventory_updates = (
            update_existing_inventory(
                cursor
            )
        )

        # -------------------------------------------------
        # Create new master/catalog records
        # -------------------------------------------------

        new_supplier_ids = (
            create_suppliers(
                cursor
            )
        )

        new_store_ids = (
            create_stores(
                cursor
            )
        )

        # Use all available suppliers,
        # including newly created ones.
        supplier_ids = get_supplier_ids(
            cursor
        )

        store_ids = get_store_ids(
            cursor
        )

        product_ids = create_products(
            cursor,
            supplier_ids,
        )

        inventory_count = (
            create_inventory_for_new_products(
                cursor,
                product_ids,
                store_ids,
            )
        )

        connection.commit()

        print()
        print(
            "RetailPulse MySQL automated "
            "batch completed."
        )
        print(
            "--------------------------------"
            "----------------"
        )

        print()
        print("NEW RECORDS")
        print(
            f"Suppliers inserted : "
            f"{len(new_supplier_ids)}"
        )
        print(
            f"Stores inserted    : "
            f"{len(new_store_ids)}"
        )
        print(
            f"Products inserted  : "
            f"{len(product_ids)}"
        )
        print(
            f"Inventory inserted : "
            f"{inventory_count}"
        )

        print()
        print("UPDATED RECORDS")
        print(
            f"Suppliers updated  : "
            f"{supplier_updates}"
        )
        print(
            f"Stores updated     : "
            f"{store_updates}"
        )
        print(
            f"Products updated   : "
            f"{product_updates}"
        )
        print(
            f"Inventory updated  : "
            f"{inventory_updates}"
        )

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    generate_data()