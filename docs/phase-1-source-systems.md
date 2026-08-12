# Phase 1 — Local Source Systems: PostgreSQL + MySQL

## RetailPulse Enterprise Data Engineering Platform

# Project Abstract

RetailPulse is an enterprise-style data engineering project designed to simulate how a modern retail and e-commerce organization collects, integrates, processes, and analyzes data from multiple operational systems.

The project begins with two independent transactional source systems:

- PostgreSQL for customer, order, order-item, and payment activity
- MySQL for supplier, product, store, and inventory activity

These systems intentionally represent separate business applications rather than one combined database. The data engineering platform will later extract data from both systems, land it in AWS, apply validation and transformation logic, and load curated analytical data into Snowflake.

The project is designed to demonstrate practical data engineering concepts including:

- multi-source ingestion
- Dockerized source systems
- relational modeling and referential integrity
- secure configuration handling
- Python-based synthetic data generation
- incremental processing readiness
- data quality validation
- cross-system integration
- AWS data lake ingestion
- PySpark/Glue transformations
- Snowflake warehouse loading
- Git-based development workflow

The final architecture will evolve toward:

```text
PostgreSQL + MySQL
        ↓
Python / ingestion services
        ↓
AWS S3
        ↓
Raw / Standardized / Curated zones
        ↓
AWS Glue / PySpark
        ↓
Snowflake
        ↓
Business analytics / reporting / future ML use cases
```

---

# Business Problem

RetailPulse operates across multiple systems that were built for different operational purposes.

The Order Management System records:

- customers
- orders
- order line items
- payments

The Product and Inventory Management System records:

- suppliers
- product catalog information
- stores
- inventory balances

Because these systems are independent, business users cannot easily answer questions that require data from both systems.

Examples:

- Which products generate the most revenue?
- Which product categories are most frequently ordered?
- Which stores have low inventory for high-demand products?
- Which suppliers support the highest-selling products?
- Are customers ordering products that are currently low in stock?
- What is the relationship between order demand and available inventory?
- Which products should be prioritized for restocking?
- How do order volume, payment status, and inventory levels change over time?

A centralized data engineering platform is required to combine these operational datasets into a consistent analytical model.

---

# Business Requirements

## BR-01 — Separate Operational Source Systems

The solution must simulate multiple independent enterprise source applications.

Required systems:

```text
PostgreSQL
→ Order Management System

MySQL
→ Product & Inventory Management System
```

This ensures the project includes realistic cross-system data integration.

---

## BR-02 — Order Management Data

The PostgreSQL source must capture:

```text
customers
orders
order_items
payments
```

The system must preserve valid relationships between customers, orders, order items, and payments through primary and foreign keys.

---

## BR-03 — Product and Inventory Data

The MySQL source must capture:

```text
suppliers
products
stores
inventory
```

The system must preserve relationships between suppliers, products, stores, and inventory.

---

## BR-04 — Cross-System Product Integration

Order data must be logically related to product master data using:

```text
PostgreSQL:
retail.order_items.product_id

        ↓

MySQL:
products.product_id
```

The common product range used during Phase 1 is:

```text
1001–1050
```

This logical business key will later be used by AWS/Glue transformations to enrich order data with product, supplier, and inventory attributes.

---

## BR-05 — Reproducible Local Development Environment

The source systems must be reproducible on another developer's machine.

Therefore the project must use:

- Docker
- Docker Compose
- official PostgreSQL and MySQL images
- persistent volumes
- a shared Docker network
- environment-variable-based configuration

A developer should be able to recreate the environment from repository files rather than manually installing and configuring the databases.

---

## BR-06 — Secure Credential Handling

Database credentials must not be committed directly to GitHub.

The solution must use:

```text
.env
→ actual local values
→ excluded from Git

.env.example
→ placeholder configuration
→ committed to Git
```

Docker Compose and Python scripts must read configuration from environment variables rather than hardcoding passwords in source code.

---

## BR-07 — Synthetic Source Data

The project must generate enough realistic source data to test ingestion and downstream transformation logic.

PostgreSQL generator requirements include:

- realistic customers
- multiple orders per customer
- multiple items per order
- payment status variations
- product IDs that correspond to the MySQL product master

MySQL generator requirements include:

- suppliers
- stores
- products
- inventory records by product/store combination

---

## BR-08 — Referential Integrity

The databases must prevent invalid child records.

Examples:

```text
An order must reference a valid customer.
An order item must reference a valid order.
A payment must reference a valid order.
A product may reference a valid supplier.
Inventory must reference valid products and stores.
```

PK/FK validation queries must return zero orphan records.

---

## BR-09 — Data Quality and Reconciliation

The generated source data must support validation checks.

Examples include:

```text
orders.order_total
=
SUM(order_items.quantity × order_items.unit_price)
```

and:

```text
No orphan orders
No orphan order items
No orphan payments
No orphan products
No orphan inventory records
```

---

## BR-10 — Incremental Processing Readiness

Source tables must include appropriate timestamps such as:

```text
created_at
updated_at
order_timestamp
payment_timestamp
last_restocked_at
```

These fields will later support:

- incremental extraction
- watermark logic
- change detection
- batch processing

---

## BR-11 — Version-Controlled Development

All project code and configuration, except secrets and local environments, must be maintained in Git.

Development must occur on a feature branch:

```text
feature/source-systems
```

The project should follow:

```text
edit
→ git status
→ git add
→ git commit
→ git push
```

before changes are eventually reviewed and merged into the main branch.

---

# Phase 1 Objective

Phase 1 establishes and validates the complete local source layer before any AWS ingestion begins.

By the end of this phase, the following must be operational:

```text
PostgreSQL source system
MySQL source system
Dockerized runtime
Persistent storage
Secure local credentials
SQL schema scripts
Sample test data
Python generators
PK/FK validation
Cross-database product-key validation
Git-based source control
```

Once these requirements are met, the project is ready to move into AWS ingestion and data-lake engineering.

---

# 1. Phase Goal

RetailPulse uses two independent operational source systems to simulate a realistic enterprise environment.

```text
PostgreSQL — Order Management System
├── customers
├── orders
├── order_items
└── payments

MySQL — Product & Inventory Management System
├── suppliers
├── products
├── stores
└── inventory
```

The main cross-system business key is:

```text
PostgreSQL
retail.order_items.product_id
        ↓
logical cross-system relationship
        ↓
MySQL
products.product_id
```

Both systems use product IDs in the range `1001–1050`.

---

# 2. Repository Structure

```text
retailpulse-enterprise-data-platform/
│
├── .env
├── .env.example
├── .gitignore
├── requirements.txt
│
├── docker/
│   └── source-systems/
│       └── docker-compose.yml
│
├── sql/
│   ├── postgres/
│   │   ├── 01_create_schema.sql
│   │   └── 02_test_data.sql
│   └── mysql/
│       ├── 01_create_schema.sql
│       └── 02_test_data.sql
│
├── src/
│   └── data_generator/
│       ├── generate_postgres_data.py
│       └── generate_mysql_data.py
│
└── docs/
    └── phase-1-source-systems.md
```

---

# 3. Docker Images and Containers

## 3.1 Why Docker

Docker is used instead of installing PostgreSQL and MySQL directly on Windows because it provides:

- isolated database environments
- reproducible versions
- easier setup and teardown
- predictable port mappings
- persistent storage through Docker volumes
- shared networking between services
- easier onboarding for another developer

## 3.2 PostgreSQL Image

Official image:

```text
postgres:17
```

Container:

```text
retailpulse-postgres
```

## 3.3 MySQL Image

Official image:

```text
mysql:8.4
```

Container:

```text
retailpulse-mysql
```

We did not create custom database images with a Dockerfile. Docker Compose uses the official images and pulls them automatically if they are not already available locally.

---

# 4. Secure Configuration and Password Handling

## 4.1 `.env`

The real local credentials are stored in `.env`.

Example structure:

```env
POSTGRES_DB=retailpulse_orders
POSTGRES_USER=retailpulse
POSTGRES_PASSWORD=<local-postgres-password>
POSTGRES_PORT=5433

MYSQL_DATABASE=retailpulse_inventory
MYSQL_USER=retailpulse
MYSQL_PASSWORD=<local-mysql-password>
MYSQL_ROOT_PASSWORD=<local-root-password>
MYSQL_PORT=3307
```

Important: `.env` does not encrypt or encode passwords. The protection comes from keeping the file local and excluding it from Git.

## 4.2 `.env.example`

Safe template committed to Git:

```env
POSTGRES_DB=retailpulse_orders
POSTGRES_USER=retailpulse
POSTGRES_PASSWORD=change_me
POSTGRES_PORT=5433

MYSQL_DATABASE=retailpulse_inventory
MYSQL_USER=retailpulse
MYSQL_PASSWORD=change_me
MYSQL_ROOT_PASSWORD=change_me
MYSQL_PORT=3307
```

## 4.3 `.gitignore`

Important entries:

```gitignore
.env
.venv/
__pycache__/
*.pyc
.vscode/
*.log
.DS_Store
Thumbs.db
```

Verify `.env` is ignored:

```powershell
git check-ignore -v .env
```

---

# 5. Docker Compose Configuration

```yaml
services:
  postgres:
    image: postgres:17
    container_name: retailpulse-postgres
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - retailpulse-network

  mysql:
    image: mysql:8.4
    container_name: retailpulse-mysql
    environment:
      MYSQL_DATABASE: ${MYSQL_DATABASE}
      MYSQL_USER: ${MYSQL_USER}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
    ports:
      - "${MYSQL_PORT}:3306"
    volumes:
      - mysql_data:/var/lib/mysql
    networks:
      - retailpulse-network

volumes:
  postgres_data:
  mysql_data:

networks:
  retailpulse-network:
    driver: bridge
```

---

# 6. Validate Docker Compose

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml config
```

What it does:

- validates YAML
- resolves environment variables
- catches indentation errors
- catches missing variables
- shows the final configuration Docker will use

It does not start containers.

---

# 7. Start the Source Systems

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml up -d
```

What it does:

```text
read docker-compose.yml
      ↓
read .env
      ↓
check/pull postgres:17 and mysql:8.4
      ↓
create/recreate containers if required
      ↓
attach volumes and network
      ↓
publish ports
      ↓
start in detached mode
```

`-d` means detached/background mode.

---

# 8. Verify Running Containers

```powershell
docker ps
```

Expected:

```text
retailpulse-postgres
retailpulse-mysql
```

Port mappings:

```text
PostgreSQL: Windows 5433 → Container 5432
MySQL:      Windows 3307 → Container 3306
```

---

# 9. Useful Docker Commands

Start an existing container:

```powershell
docker start retailpulse-postgres
docker start retailpulse-mysql
```

Stop:

```powershell
docker stop retailpulse-postgres
docker stop retailpulse-mysql
```

Restart:

```powershell
docker restart retailpulse-postgres
docker restart retailpulse-mysql
```

Stop Compose services:

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml down
```

Do not casually use:

```powershell
docker compose down -v
```

`-v` removes named volumes and can delete persistent database data.

---

# 10. Persistent Storage

PostgreSQL:

```text
postgres_data → /var/lib/postgresql/data
```

MySQL:

```text
mysql_data → /var/lib/mysql
```

A Docker volume is persistent storage, not a true backup.

---

# 11. PostgreSQL Connection

Database:

```text
retailpulse_orders
```

DBeaver:

```text
Host: 127.0.0.1
Port: 5433
Database: retailpulse_orders
Username: retailpulse
Password: local .env value
```

Direct CLI:

```powershell
docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse_orders
```

---

# 12. PostgreSQL Password Authentication Test

```powershell
docker exec -e PGPASSWORD=<local-password> retailpulse-postgres `
  psql -h 127.0.0.1 -U retailpulse -d retailpulse_orders `
  -c "SELECT current_user, current_database();"
```

Using `-h 127.0.0.1` forces TCP authentication instead of relying only on a local socket connection.

---

# 13. PostgreSQL Schema Creation

File:

```text
sql/postgres/01_create_schema.sql
```

Dedicated schema:

```text
retail
```

Run:

```powershell
Get-Content ".\sql\postgres\01_create_schema.sql" |
docker exec -i retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders
```

Verify schemas:

```powershell
docker exec -it retailpulse-postgres `
  psql -U retailpulse -d retailpulse_orders -c "\dn"
```

Verify tables:

```powershell
docker exec -it retailpulse-postgres `
  psql -U retailpulse -d retailpulse_orders -c "\dt retail.*"
```

Expected tables:

```text
customers
orders
order_items
payments
```

---

# 14. PostgreSQL Entity Relationships

```text
customers
   PK customer_id
        |
        | 1 : many
        v
orders
   PK order_id
   FK customer_id
        |
        +----------------------+
        |                      |
        v                      v
order_items                payments
PK order_item_id           PK payment_id
FK order_id                FK order_id
product_id
```

Relationship summary:

| Parent | Parent PK | Child | Child FK | Relationship |
|---|---|---|---|---|
| customers | customer_id | orders | customer_id | 1 : many |
| orders | order_id | order_items | order_id | 1 : many |
| orders | order_id | payments | order_id | 1 : many |

`order_items.product_id` is intentionally not a PostgreSQL FK because the product master belongs to MySQL.

---

# 15. PostgreSQL Sample Test Data

File:

```text
sql/postgres/02_test_data.sql
```

Concept:

```text
insert customer
      ↓
RETURNING customer_id
      ↓
insert order
      ↓
RETURNING order_id
      ↓
insert order_item + payment
```

Run:

```powershell
Get-Content ".\sql\postgres\02_test_data.sql" |
docker exec -i retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders
```

This avoids hardcoding generated FK values.

---

# 16. Python Virtual Environment (`.venv`)

## 16.1 What is `.venv`?

`.venv` is a **project-specific Python virtual environment**.

It creates an isolated Python environment inside the project so the libraries required by RetailPulse do not interfere with Python packages installed for other projects on the same computer.

Conceptually:

```text
Windows Python installation
        ↓
py -m venv .venv
        ↓
RetailPulse-specific Python environment
        ↓
psycopg
Faker
python-dotenv
mysql-connector-python
```

The `.venv` folder is **not the Python data generator itself**.

It is the isolated environment that provides the Python runtime and libraries used by the generator scripts.

---

## 16.2 Why We Created `.venv`

We created `.venv` for several reasons:

- isolate RetailPulse Python packages from other projects
- avoid dependency/version conflicts
- keep project setup reproducible
- make it easy for another developer to recreate the environment
- prevent unnecessary Python package folders from being committed to Git
- keep the project closer to standard professional Python development practices

For example, one project might require one version of a package while another project needs a different version. Virtual environments prevent those packages from conflicting.

---

## 16.3 Create the Virtual Environment

From the project root:

```powershell
py -m venv .venv
```

Command explanation:

```text
py
→ Windows Python Launcher

-m
→ tells Python to run a module

venv
→ Python's built-in virtual-environment module

.venv
→ folder where the isolated environment is created
```

Result:

```text
retailpulse-enterprise-data-platform/
└── .venv/
```

---

## 16.4 Activate the Virtual Environment

PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Command explanation:

```text
.\ 
→ current project directory

.venv
→ virtual-environment folder

Scripts
→ Windows-specific executables/activation scripts

Activate.ps1
→ activates the environment in PowerShell
```

After activation, the terminal prompt begins with:

```text
(.venv)
```

Example:

```text
(.venv) PS C:\Users\...\retailpulse-enterprise-data-platform>
```

This confirms commands such as `pip` and Python scripts are using the RetailPulse virtual environment.

---

## 16.5 Verify Python Version

```powershell
py --version
```

Verified during setup:

```text
Python 3.13.15
```

---

## 16.6 Deactivate the Virtual Environment

When Python work is finished:

```powershell
deactivate
```

This returns the terminal to the normal Windows Python environment.

Important:

Docker and Git commands can still be executed while `(.venv)` is active.  
The virtual environment only affects Python-related commands.

Examples that still work normally inside `(.venv)`:

```powershell
docker ps
docker compose ...
git status
git add ...
git commit ...
git push
```

---

## 16.7 Why `.venv` Is Not Pushed to GitHub

`.venv` contains installed Python packages and environment-specific files.

It can be recreated from:

```text
requirements.txt
```

Therefore `.gitignore` contains:

```gitignore
.venv/
```

Another developer can recreate it with:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

---

# 17. Python Libraries

Install PostgreSQL libraries:

```powershell
pip install "psycopg[binary]" Faker python-dotenv
```

Install MySQL connector:

```powershell
pip install mysql-connector-python
```

Purpose:

| Library | Purpose |
|---|---|
| psycopg[binary] | Python → PostgreSQL |
| mysql-connector-python | Python → MySQL |
| Faker | synthetic business data |
| python-dotenv | loads `.env` values |

`requirements.txt`:

```text
psycopg[binary]
Faker
python-dotenv
mysql-connector-python
```

Reinstall later with:

```powershell
pip install -r requirements.txt
```

---

# 18. PostgreSQL Python Generator

File:

```text
src/data_generator/generate_postgres_data.py
```

Flow:

```text
.env
  ↓
python-dotenv
  ↓
psycopg
  ↓
PostgreSQL
  ↓
customers
  ↓
orders
  ↓
order_items
  ↓
payments
```

Important SQL:

```sql
RETURNING customer_id;
RETURNING order_id;
```

These return actual PostgreSQL-generated primary keys back to Python.

Run:

```powershell
py src\data_generator\generate_postgres_data.py
```

A successful 1,000-customer run produced:

```text
Customers inserted : 1000
Orders inserted    : 1987
Order items        : 3988
Payments inserted  : 1466
```

Generated `order_items.product_id` values use:

```text
1001–1050
```

---

# 19. PostgreSQL Validation

Counts:

```sql
SELECT COUNT(*) FROM retail.customers;
SELECT COUNT(*) FROM retail.orders;
SELECT COUNT(*) FROM retail.order_items;
SELECT COUNT(*) FROM retail.payments;
```

Orphan checks:

```sql
SELECT COUNT(*) AS orphan_orders
FROM retail.orders o
LEFT JOIN retail.customers c
  ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

```sql
SELECT COUNT(*) AS orphan_order_items
FROM retail.order_items oi
LEFT JOIN retail.orders o
  ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;
```

```sql
SELECT COUNT(*) AS orphan_payments
FROM retail.payments p
LEFT JOIN retail.orders o
  ON p.order_id = o.order_id
WHERE o.order_id IS NULL;
```

Expected:

```text
0
```

Order total reconciliation:

```sql
SELECT
    o.order_id,
    o.order_total,
    SUM(oi.quantity * oi.unit_price) AS calculated_total
FROM retail.orders o
JOIN retail.order_items oi
  ON o.order_id = oi.order_id
GROUP BY o.order_id, o.order_total
HAVING o.order_total <> SUM(oi.quantity * oi.unit_price);
```

Expected:

```text
0 rows
```

---

# 20. MySQL Connection

Database:

```text
retailpulse_inventory
```

DBeaver:

```text
Host: 127.0.0.1
Port: 3307
Database: retailpulse_inventory
Username: retailpulse
Password: local .env value
```

---

# 21. DBeaver MySQL Authentication Fixes

Initial error:

```text
Public Key Retrieval is not allowed
```

Set:

```text
allowPublicKeyRetrieval = true
```

A later truststore error showed:

```text
trustAnchors parameter must be non-empty
```

For this local-only Docker connection:

```text
Require SSL:                unchecked
Verify server certificate:  unchecked
Allow public key retrieval: checked
```

Production environments should use proper TLS configuration rather than disabling certificate verification.

---

# 22. MySQL Schema Creation

File:

```text
sql/mysql/01_create_schema.sql
```

Run:

```powershell
Get-Content ".\sql\mysql\01_create_schema.sql" |
docker exec -i retailpulse-mysql `
mysql -u retailpulse -p<local-password> retailpulse_inventory
```

Verify:

```powershell
docker exec -it retailpulse-mysql `
mysql -u retailpulse -p<local-password> `
-D retailpulse_inventory `
-e "SHOW TABLES;"
```

Expected:

```text
inventory
products
stores
suppliers
```

---

# 23. MySQL Entity Relationships

```text
suppliers
   PK supplier_id
        |
        | 1 : many
        v
products
   PK product_id
   FK supplier_id
        |
        | 1 : many
        v
inventory
   PK inventory_id
   FK product_id
   FK store_id
        ^
        |
        | many : 1
        |
stores
   PK store_id
```

Relationship summary:

| Parent | Parent PK | Child | Child FK | Relationship |
|---|---|---|---|---|
| suppliers | supplier_id | products | supplier_id | 1 : many |
| products | product_id | inventory | product_id | 1 : many |
| stores | store_id | inventory | store_id | 1 : many |

`inventory` also has a unique constraint on:

```text
(product_id, store_id)
```

This prevents duplicate inventory rows for the same product/store pair.

---

# 24. Verify MySQL Foreign Keys

```powershell
docker exec -it retailpulse-mysql `
mysql -u retailpulse -p<local-password> `
-D retailpulse_inventory `
-e "SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='retailpulse_inventory' AND REFERENCED_TABLE_NAME IS NOT NULL;"
```

Confirmed:

```text
inventory.product_id → products.product_id
inventory.store_id   → stores.store_id
products.supplier_id → suppliers.supplier_id
```

---

# 25. MySQL Sample Test Data

File:

```text
sql/mysql/02_test_data.sql
```

Test data inserted:

```text
1 supplier
1 product
1 store
1 inventory row
```

MySQL generated IDs were captured with:

```sql
LAST_INSERT_ID()
```

Concept:

```text
insert supplier
      ↓
LAST_INSERT_ID()
      ↓
products.supplier_id

insert store
      ↓
LAST_INSERT_ID()
      ↓
inventory.store_id
```

The test product used:

```text
product_id = 1001
```

---

# 26. MySQL Development Reset

Before running the bulk generator, the sample data was cleared to avoid a duplicate product ID `1001`.

```sql
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE inventory;
TRUNCATE TABLE products;
TRUNCATE TABLE stores;
TRUNCATE TABLE suppliers;

SET FOREIGN_KEY_CHECKS = 1;
```

This removes rows but keeps table definitions.

---

# 27. MySQL Python Generator

File:

```text
src/data_generator/generate_mysql_data.py
```

Configuration:

```text
10 suppliers
10 stores
50 products
product_id range 1001–1050
500 inventory rows
```

Inventory logic:

```text
50 products × 10 stores = 500 inventory rows
```

The script uses:

```python
cursor.lastrowid
```

to capture generated supplier/store IDs.

Run:

```powershell
py src\data_generator\generate_mysql_data.py
```

Observed result:

```text
Suppliers inserted : 10
Stores inserted    : 10
Products inserted  : 50
Inventory rows     : 500
```

---

# 28. MySQL Validation

Counts:

```sql
SELECT COUNT(*) AS suppliers FROM suppliers;
SELECT COUNT(*) AS stores FROM stores;
SELECT COUNT(*) AS products FROM products;
SELECT COUNT(*) AS inventory_rows FROM inventory;
```

Expected:

```text
suppliers = 10
stores = 10
products = 50
inventory = 500
```

Orphan checks:

```sql
SELECT COUNT(*) AS orphan_products
FROM products p
LEFT JOIN suppliers s
  ON p.supplier_id = s.supplier_id
WHERE s.supplier_id IS NULL;
```

```sql
SELECT COUNT(*) AS orphan_inventory_products
FROM inventory i
LEFT JOIN products p
  ON i.product_id = p.product_id
WHERE p.product_id IS NULL;
```

```sql
SELECT COUNT(*) AS orphan_inventory_stores
FROM inventory i
LEFT JOIN stores s
  ON i.store_id = s.store_id
WHERE s.store_id IS NULL;
```

Expected:

```text
0
```

---

# 29. Cross-Database Relationship

There is no physical FK between PostgreSQL and MySQL because they are separate database engines.

Logical relationship:

```text
PostgreSQL
retail.order_items.product_id
        ↓
MySQL
products.product_id
```

PostgreSQL:

```sql
SELECT DISTINCT product_id
FROM retail.order_items
ORDER BY product_id;
```

Observed range:

```text
1001–1050
```

MySQL contains exactly:

```text
50 products
product_id 1001–1050
```

Therefore the product business key is aligned across both source systems.

---

# 30. Cross-System Architecture

```text
PostgreSQL                              MySQL
Order Management                       Product / Inventory
----------------                       -------------------
customers                              suppliers
    |                                      |
    v                                      v
orders                                 products
    |                                      |
    +----------+                           v
    |          |                        inventory
    v          v                           ^
order_items  payments                      |
    |                                      |
    | product_id                           |
    +---------------- product_id ----------+
                                           |
                                         stores
```

Later:

```text
PostgreSQL + MySQL
        ↓
AWS ingestion
        ↓
S3 raw layer
        ↓
Glue / PySpark
        ↓
cross-source joins
        ↓
Snowflake
```

---

# 31. Troubleshooting History

## Docker / WSL2

Docker initially could not start correctly because WSL2 was missing.

```powershell
wsl --install
```

## Docker Compose YAML Indentation

MySQL was accidentally nested under the network section.

Error:

```text
networks.retailpulse-network additional properties ...
```

Fix:

```yaml
services:
  postgres:
  mysql:

volumes:
  postgres_data:
  mysql_data:

networks:
  retailpulse-network:
```

## Missing MySQL `.env` Variables

Compose warned that MySQL variables were unset.

Fix: add and save:

```text
MYSQL_DATABASE
MYSQL_USER
MYSQL_PASSWORD
MYSQL_ROOT_PASSWORD
MYSQL_PORT
```

## `.env` Accidentally Created as a Directory

`mkdir .env` created a directory instead of a file.

Fix: remove it and create a real `.env` file.

## PostgreSQL Password Authentication

Password issues were tested over TCP using `psql -h 127.0.0.1`.

## PostgreSQL Schema Missing

`\dn` showed only `public`.

Fix: rerun `01_create_schema.sql` against the active container.

## Wrong PowerShell Working Directory

`Get-Content` could not find the SQL file when PowerShell was inside `docker\source-systems`.

Fix: use the correct project-root-relative path.

## SQL File vs Directory

This failed:

```powershell
cd 01_create_schema.sql
```

because `.sql` is a file, not a directory.

## Hardcoded PostgreSQL IDs

Hardcoded IDs caused FK failures after sequences advanced.

Fix:

```sql
RETURNING customer_id
RETURNING order_id
```

## Python Command Not Found

`python` was not resolved correctly through Windows PATH.

Fix:

```powershell
py -m venv .venv
```

## MySQL Public Key Retrieval

Fix:

```text
allowPublicKeyRetrieval = true
```

## MySQL Truststore Error

Local development fix:

```text
Require SSL:                unchecked
Verify server certificate:  unchecked
Allow public key retrieval: checked
```

---

# 32. Git Workflow

Current branch:

```text
feature/source-systems
```

Check changes:

```powershell
git status
```

Stage safe files:

```powershell
git add .env.example .gitignore requirements.txt docker sql src docs
```

Commit:

```powershell
git commit -m "feat: add MySQL product and inventory source system"
```

Push:

```powershell
git push
```

Because `feature/source-systems` already tracks the remote branch, a normal `git push` is sufficient.

Flow:

```text
local edits
   ↓
git status
   ↓
git add
   ↓
git commit
   ↓
git push
   ↓
GitHub feature/source-systems
```

Never stage or push `.env`.

---

# 33. Phase 1 Completion Status

```text
Docker / WSL2                             COMPLETE
PostgreSQL official image                COMPLETE
PostgreSQL container                     COMPLETE
PostgreSQL persistent volume             COMPLETE
PostgreSQL DBeaver connection            COMPLETE
PostgreSQL schema                        COMPLETE
PostgreSQL test data                     COMPLETE
PostgreSQL Python generator              COMPLETE
PostgreSQL validation                    COMPLETE

MySQL official image                     COMPLETE
MySQL container                          COMPLETE
MySQL persistent volume                  COMPLETE
MySQL DBeaver connection                 COMPLETE
MySQL schema                             COMPLETE
MySQL FK validation                      COMPLETE
MySQL sample data                        COMPLETE
MySQL Python generator                   COMPLETE
MySQL validation                         COMPLETE

Shared Docker network                    COMPLETE
.env / .env.example handling             COMPLETE
Python virtual environment               COMPLETE
requirements.txt                         COMPLETE
Cross-system product key 1001–1050       COMPLETE
Git feature branch workflow              COMPLETE
```

---


---

# 35. Important Run Commands — Quick Operational Reference

This section contains the most important commands required to run the complete local source-system environment from a fresh terminal.

## 35.1 Go to the Project Root

```powershell
cd C:\Users\saite\aws-data-engineering\retailpulse-enterprise-data-platform
```

Purpose:

Ensures relative paths such as `docker/source-systems/docker-compose.yml` and `src/data_generator/...` resolve correctly.

---

## 35.2 Activate Python Environment

```powershell
.\.venv\Scripts\Activate.ps1
```

Expected:

```text
(.venv)
```

---

## 35.3 Validate Docker Compose

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml config
```

Purpose:

Checks YAML syntax and confirms `.env` variables are being resolved correctly.

Run this before `up -d` whenever `docker-compose.yml` or `.env` changes.

---

## 35.4 Start PostgreSQL and MySQL

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml up -d
```

Purpose:

Starts both source-system containers in the background.

---

## 35.5 Verify Containers

```powershell
docker ps
```

Expected:

```text
retailpulse-postgres
retailpulse-mysql
```

Expected ports:

```text
PostgreSQL: 5433 → 5432
MySQL:      3307 → 3306
```

---

## 35.6 Start a Single Existing Container

PostgreSQL:

```powershell
docker start retailpulse-postgres
```

MySQL:

```powershell
docker start retailpulse-mysql
```

Use this when a container already exists but is stopped.

---

## 35.7 Stop a Single Container

```powershell
docker stop retailpulse-postgres
docker stop retailpulse-mysql
```

---

## 35.8 Restart a Single Container

```powershell
docker restart retailpulse-postgres
docker restart retailpulse-mysql
```

---

## 35.9 Stop the Compose Environment

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml down
```

This removes the containers/network created by Compose but keeps named volumes.

Do not casually use:

```powershell
docker compose down -v
```

because `-v` can remove persistent database data.

---

## 35.10 Run PostgreSQL Schema Script

From the project root:

```powershell
Get-Content ".\sql\postgres\01_create_schema.sql" |
docker exec -i retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders
```

Purpose:

Creates the `retail` schema, four PostgreSQL tables, constraints, and indexes.

---

## 35.11 Run PostgreSQL Sample Test Data

```powershell
Get-Content ".\sql\postgres\02_test_data.sql" |
docker exec -i retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders
```

Purpose:

Inserts a small relational test transaction used to validate PK/FK behavior.

---

## 35.12 Run PostgreSQL Data Generator

```powershell
py src\data_generator\generate_postgres_data.py
```

Purpose:

Generates and inserts:

```text
customers
orders
order_items
payments
```

The script reads PostgreSQL connection values from `.env`.

---

## 35.13 Verify PostgreSQL Tables

```powershell
docker exec -it retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders `
-c "\dt retail.*"
```

---

## 35.14 Enter PostgreSQL CLI

```powershell
docker exec -it retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders
```

Exit:

```text
\q
```

---

## 35.15 Run MySQL Schema Script

```powershell
Get-Content ".\sql\mysql\01_create_schema.sql" |
docker exec -i retailpulse-mysql `
mysql -u retailpulse -p<local-password> retailpulse_inventory
```

Purpose:

Creates:

```text
suppliers
products
stores
inventory
```

Replace `<local-password>` with the local MySQL application password when executing manually.

Do not put the real password into committed documentation.

---

## 35.16 Run MySQL Sample Test Data

```powershell
Get-Content ".\sql\mysql\02_test_data.sql" |
docker exec -i retailpulse-mysql `
mysql -u retailpulse -p<local-password> retailpulse_inventory
```

Purpose:

Creates a small supplier/product/store/inventory test relationship.

---

## 35.17 Run MySQL Data Generator

```powershell
py src\data_generator\generate_mysql_data.py
```

Purpose:

Generates:

```text
10 suppliers
10 stores
50 products
500 inventory records
```

The script reads MySQL connection values from `.env`.

---

## 35.18 Verify MySQL Tables

```powershell
docker exec -it retailpulse-mysql `
mysql -u retailpulse -p<local-password> `
-D retailpulse_inventory `
-e "SHOW TABLES;"
```

---

## 35.19 Enter MySQL CLI

```powershell
docker exec -it retailpulse-mysql `
mysql -u retailpulse -p<local-password> `
-D retailpulse_inventory
```

Exit:

```text
exit
```

---

## 35.20 Verify `.env` Is Protected

```powershell
git check-ignore -v .env
```

Purpose:

Confirms the real local credential file will not be tracked by Git.

---

## 35.21 Check Local Git Changes

```powershell
git status
```

Purpose:

Shows:

- modified files
- new/untracked files
- staged files
- deleted files

---

## 35.22 Stage Source-System Changes

```powershell
git add .env.example .gitignore requirements.txt docker sql src docs
```

Important:

`.env` must not appear in staged files.

---

## 35.23 Commit

Example:

```powershell
git commit -m "feat: add MySQL product and inventory source system"
```

`-m` means commit **message**.

---

## 35.24 Push

```powershell
git push
```

Purpose:

Pushes committed changes from the local `feature/source-systems` branch to GitHub.

---

# 36. Typical Daily Startup Sequence

When returning to the project on another day, the normal startup sequence is:

```powershell
cd C:\Users\saite\aws-data-engineering\retailpulse-enterprise-data-platform

.\.venv\Scripts\Activate.ps1

docker compose --env-file .env -f docker/source-systems/docker-compose.yml up -d

docker ps
```

Then verify DBeaver connections:

```text
PostgreSQL → 127.0.0.1:5433
MySQL      → 127.0.0.1:3307
```

At that point the complete Phase 1 local source environment is ready.

---

# 37. Typical Full Rebuild Sequence

If containers need to be recreated while preserving database volumes:

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml down

docker compose --env-file .env -f docker/source-systems/docker-compose.yml up -d

docker ps
```

Because the named volumes are kept, existing database files should remain available.

If starting from empty volumes, deploy the schemas:

```powershell
Get-Content ".\sql\postgres\01_create_schema.sql" |
docker exec -i retailpulse-postgres `
psql -U retailpulse -d retailpulse_orders
```

```powershell
Get-Content ".\sql\mysql\01_create_schema.sql" |
docker exec -i retailpulse-mysql `
mysql -u retailpulse -p<local-password> retailpulse_inventory
```

Then run the generators:

```powershell
py src\data_generator\generate_postgres_data.py
py src\data_generator\generate_mysql_data.py
```

Finally validate both databases in DBeaver or with CLI queries.

# 34. Next Phase

```text
PostgreSQL / MySQL
        ↓
Python extraction
        ↓
incremental/watermark logic
        ↓
AWS S3
        ↓
raw / standardized / curated
        ↓
Glue / PySpark
        ↓
Snowflake
```
