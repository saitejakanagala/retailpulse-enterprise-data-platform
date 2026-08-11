# Phase 1A — PostgreSQL Order Management Source System

## RetailPulse Enterprise AWS + Snowflake Data Engineering Platform

## 1. Purpose

This phase builds the first operational source system for RetailPulse: a PostgreSQL-based Order Management System running locally in Docker. It simulates a realistic enterprise source database that will later feed the AWS data engineering pipeline.

```text
Python Data Generator
        |
        v
PostgreSQL in Docker
        |
        v
retailpulse_orders
        |
        v
retail schema
  |- customers
  |- orders
  |- order_items
  |- payments
        |
        v
Future AWS ingestion -> S3 -> Glue/PySpark -> Snowflake
```

## 2. Repository Structure

```text
retailpulse-enterprise-data-platform/
|
|- .env                     # local only; never commit
|- .env.example             # safe template committed to Git
|- .gitignore
|- requirements.txt
|
|- docker/
|  `- source-systems/
|     `- docker-compose.yml
|
|- sql/
|  |- mysql/
|  `- postgres/
|     |- 01_create_schema.sql
|     `- 02_test_data.sql
|
|- src/
|  `- data_generator/
|     `- generate_postgres_data.py
|
|- docs/
|- README.md
`- RetailPulse_Phase_0_Engineering_Setup.md
```

## 3. Docker Architecture

- **Image:** `postgres:17`
- **Container:** `retailpulse-postgres`
- **Host-to-container port:** `5433 -> 5432`
- **Persistent volume:** `source-systems_postgres_data -> /var/lib/postgresql/data`
- **Docker network:** `source-systems_retailpulse-network`

An image is the reusable template. A container is the running instance. The volume stores PostgreSQL data independently of the container lifecycle.

## 4. Docker Compose Configuration

The project uses environment-variable substitution instead of hardcoding secrets.

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

volumes:
  postgres_data:

networks:
  retailpulse-network:
    driver: bridge
```

## 5. Environment Configuration

Local `.env` structure:

```text
POSTGRES_DB=retailpulse_orders
POSTGRES_USER=retailpulse
POSTGRES_PASSWORD=<local-password>
POSTGRES_PORT=5433
```

Safe `.env.example`:

```text
POSTGRES_DB=retailpulse_orders
POSTGRES_USER=retailpulse
POSTGRES_PASSWORD=change_me
POSTGRES_PORT=5433
```

`.gitignore` includes:

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

## 6. Important Docker Commands

### Validate Compose

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml config
```

Validates YAML, resolves variables, and shows the final configuration Docker will use.

### Start/Recreate Services

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml up -d
```

`up` creates/starts services and `-d` runs them in the background.

### Check Running Containers

```powershell
docker ps
```

Used to verify container status and published ports. Expected PostgreSQL mapping:

```text
0.0.0.0:5433->5432/tcp
```

### Start Existing Container

```powershell
docker start retailpulse-postgres
```

### Stop Container

```powershell
docker stop retailpulse-postgres
```

### Restart Container

```powershell
docker restart retailpulse-postgres
```

### Stop Compose Services Without Deleting Data

```powershell
docker compose --env-file .env -f docker/source-systems/docker-compose.yml down
```

Do **not** use `down -v` unless the volume should intentionally be deleted.

## 7. PostgreSQL Connection

```text
Host:     127.0.0.1
Port:     5433
Database: retailpulse_orders
Username: retailpulse
Password: stored locally
```

## 8. PostgreSQL CLI Access

```powershell
docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse_orders
```

## 9. Password Authentication Validation

Pattern used during troubleshooting:

```powershell
docker exec -e PGPASSWORD=<local-password> retailpulse-postgres psql -h 127.0.0.1 -U retailpulse -d retailpulse_orders -c "SELECT current_user, current_database();"
```

This forces TCP/password authentication and verifies the credentials directly.

## 10. Schema

RetailPulse uses a dedicated PostgreSQL schema named `retail` rather than the default `public` schema.

Verify schemas:

```powershell
docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse_orders -c "\dn"
```

Expected:

```text
public
retail
```

## 11. Entity Relationship Model

```text
CUSTOMERS
PK customer_id
     |
     | 1 : many
     v
ORDERS
PK order_id
FK customer_id
     |
     +------------------+
     |                  |
     | 1 : many         | 1 : many
     v                  v
ORDER_ITEMS          PAYMENTS
PK order_item_id     PK payment_id
FK order_id          FK order_id
product_id*
```

`product_id` is intentionally not a PostgreSQL foreign key because the future product master will live in MySQL.

## 12. Entity Details

### `retail.customers`

| Column | Key / Constraint | Purpose |
|---|---|---|
| `customer_id` | PK | Unique customer identifier |
| `first_name` |  | Customer first name |
| `last_name` |  | Customer last name |
| `email` | UNIQUE, NOT NULL | Unique customer email |
| `phone` |  | Customer phone |
| `city` |  | Customer city |
| `state` |  | Customer state |
| `country` | default USA | Customer country |
| `created_at` | timestamp | Creation time |
| `updated_at` | timestamp | Last modification time |

### `retail.orders`

| Column | Key / Constraint | Purpose |
|---|---|---|
| `order_id` | PK | Unique order |
| `customer_id` | FK -> customers.customer_id | Customer who placed the order |
| `order_status` |  | COMPLETED / SHIPPED / PENDING / CANCELLED |
| `order_total` | CHECK >= 0 | Order total |
| `order_channel` |  | WEB / MOBILE / STORE |
| `order_timestamp` | timestamp | Order creation time |
| `updated_at` | timestamp | Last modification time |

### `retail.order_items`

| Column | Key / Constraint | Purpose |
|---|---|---|
| `order_item_id` | PK | Unique line item |
| `order_id` | FK -> orders.order_id | Parent order |
| `product_id` | NOT NULL | Logical product identifier |
| `quantity` | CHECK > 0 | Quantity |
| `unit_price` | CHECK >= 0 | Unit price |
| `created_at` | timestamp | Creation time |

### `retail.payments`

| Column | Key / Constraint | Purpose |
|---|---|---|
| `payment_id` | PK | Unique payment record |
| `order_id` | FK -> orders.order_id | Parent order |
| `payment_method` |  | CARD / PAYPAL / APPLE_PAY |
| `payment_status` |  | SUCCESS / FAILED / REFUNDED |
| `amount` | CHECK >= 0 | Payment amount |
| `transaction_reference` |  | Payment reference |
| `payment_timestamp` | timestamp | Payment processing time |

## 13. Relationship Summary

| Parent | Parent PK | Child | Child FK | Relationship |
|---|---|---|---|---|
| customers | customer_id | orders | customer_id | 1 : many |
| orders | order_id | order_items | order_id | 1 : many |
| orders | order_id | payments | order_id | 1 : many |
| MySQL products (future) | product_id | PostgreSQL order_items | product_id | logical cross-system relationship |

## 14. Indexes

Indexes were created on:

```text
orders.customer_id
orders.order_timestamp
order_items.order_id
order_items.product_id
payments.order_id
```

## 15. Schema Deployment

From `sql\postgres`:

```powershell
Get-Content ".\01_create_schema.sql" | docker exec -i retailpulse-postgres psql -U retailpulse -d retailpulse_orders
```

Observed successful output:

```text
CREATE SCHEMA
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE INDEX
```

Verify tables:

```powershell
docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse_orders -c "\dt retail.*"
```

Expected:

```text
retail | customers
retail | order_items
retail | orders
retail | payments
```

## 16. Python Environment

Create virtual environment:

```powershell
py -m venv .venv
```

Activate:

```powershell
.\.venv\Scripts\Activate.ps1
```

Verified Python version:

```text
Python 3.13.15
```

## 17. Python Dependencies

Installed:

```powershell
pip install "psycopg[binary]" Faker python-dotenv
```

Purpose:

- `psycopg`: Python -> PostgreSQL
- `Faker`: realistic synthetic source data
- `python-dotenv`: load `.env` values

`requirements.txt`:

```text
psycopg[binary]
Faker
python-dotenv
```

## 18. Python Data Generator

File:

```text
src/data_generator/generate_postgres_data.py
```

The generator:

1. reads `.env`
2. connects to PostgreSQL
3. generates customers
4. uses `RETURNING customer_id`
5. generates related orders
6. uses `RETURNING order_id`
7. generates related order items
8. calculates order totals from line items
9. generates payments
10. commits the transaction
11. prints generated row counts

The use of generated IDs prevents hardcoded foreign-key errors.

## 19. Running the Generator

```powershell
py src\data_generator\generate_postgres_data.py
```

Each run appends new rows unless the source tables are intentionally reset.

## 20. Controlled Test Results

Small test run:

```text
Customers inserted : 10
Orders inserted    : 24
Order items        : 51
Payments inserted  : 17
```

Medium-scale run with `CUSTOMER_COUNT = 1000`:

```text
Customers inserted : 1000
Orders inserted    : 1980
Order items        : 3948
Payments inserted  : 1518
```

## 21. Baseline Reset During Development

Earlier test runs accumulated rows because the generator appends data. A one-time development reset was used:

```sql
TRUNCATE TABLE
    retail.payments,
    retail.order_items,
    retail.orders,
    retail.customers
RESTART IDENTITY CASCADE;
```

This removes rows, resets generated IDs, and respects FK dependencies. Future pipeline work should simulate incremental inserts/updates rather than repeatedly truncating the source.

## 22. Data Quality Validation

### Table Counts

```sql
SELECT COUNT(*) AS customers FROM retail.customers;
SELECT COUNT(*) AS orders FROM retail.orders;
SELECT COUNT(*) AS order_items FROM retail.order_items;
SELECT COUNT(*) AS payments FROM retail.payments;
```

### Orphan Orders

```sql
SELECT COUNT(*) AS orphan_orders
FROM retail.orders o
LEFT JOIN retail.customers c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

Expected: `0`

### Orphan Order Items

```sql
SELECT COUNT(*) AS orphan_order_items
FROM retail.order_items oi
LEFT JOIN retail.orders o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;
```

Expected: `0`

### Orphan Payments

```sql
SELECT COUNT(*) AS orphan_payments
FROM retail.payments p
LEFT JOIN retail.orders o
    ON p.order_id = o.order_id
WHERE o.order_id IS NULL;
```

Expected: `0`

### Order Total Reconciliation

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

Expected: `0 rows`

Business rule:

```text
orders.order_total = SUM(order_items.quantity * order_items.unit_price)
```

## 23. Troubleshooting History

### Docker Engine / WSL2

Docker initially could not start because WSL2 was not installed/configured. `wsl --install` was used, Ubuntu was provisioned under WSL2, and Docker Desktop then ran Linux containers successfully.

### Repository Naming / Clone Issue

The initial repository name/URL contained an extra dot. The repository was renamed to `retailpulse-enterprise-data-platform` and cloned successfully.

### Git Pull Conflict

A manually uploaded Phase 0 file conflicted with an untracked local copy. The duplicate local copy was removed and the repository was pulled successfully.

### PostgreSQL Password Authentication

DBeaver/Python reported `FATAL: password authentication failed for user "retailpulse"`. Authentication was diagnosed with a direct TCP/password test inside the container and the PostgreSQL role password was validated/reset as needed.

### Empty `public` Schema

DBeaver initially showed no tables under `public`. RetailPulse tables were intentionally created under the dedicated `retail` schema.

### Missing `retail` Schema

At one point `\dn` showed only `public`. The active database instance did not contain the RetailPulse schema, so `01_create_schema.sql` was rerun. The schema and four tables were recreated successfully.

### SQL File Path Error

`Get-Content` failed when run from `docker\source-systems` because the SQL file was under `sql\postgres`. The working directory/path was corrected.

### `cd 01_create_schema.sql`

This failed because `cd` changes folders, while `.sql` is a file. The correct pattern is to read/execute the file with `Get-Content` and `psql`.

### Hardcoded FK IDs

Hardcoded `customer_id=1` and `order_id=1` caused FK violations after sequence values changed. The solution was to capture IDs with PostgreSQL `RETURNING` and propagate those actual values to child rows.

### CTE Scope Issue

A test-data CTE approach produced `relation "new_order_item" does not exist`. The test logic was replaced with a PostgreSQL `DO $$ ... $$` block using variables.

### `.env` Created as a Directory

`mkdir .env` created a directory. It was removed and `.env` was recreated as a file.

### Blank Compose Variables / Random Port

When `.env` was missing, Compose warned that PostgreSQL variables were unset and Docker temporarily assigned a random host port. After creating `.env` and using `--env-file .env`, PostgreSQL returned to the intended host port 5433.

### `.gitignore` Misnamed

The ignore file was initially named `.git ignore` with a space, so Git still showed `.env`. It was renamed to `.gitignore`, `.env` was added, and `git check-ignore -v .env` was used to verify exclusion.

### `python` Command Not Found

`python -m venv .venv` was redirected to the Microsoft Store / not found via PATH. The Windows Python Launcher was used instead:

```powershell
py -m venv .venv
```

## 24. Git Workflow

Development branch:

```text
feature/source-systems
```

Before staging:

```powershell
git status
```

Verify `.env` is ignored:

```powershell
git check-ignore -v .env
```

Stage safe files:

```powershell
git add .env.example .gitignore requirements.txt docker sql src docs
```

Inspect staged content:

```powershell
git status
```

Commit:

```powershell
git commit -m "feat: build PostgreSQL source system and data generator"
```

Push feature branch:

```powershell
git push -u origin feature/source-systems
```

After the first upstream push, later pushes can use:

```powershell
git push
```

## 25. Git Safety Rule

Never commit `.env`.

Before configuration-related commits, verify:

```powershell
git status
git check-ignore -v .env
```

Only `.env.example` should be version controlled.

## 26. Phase Completion

```text
PostgreSQL Docker image                    COMPLETE
PostgreSQL container                      COMPLETE
Persistent Docker volume                  COMPLETE
Docker network                            COMPLETE
Stable host-port configuration            COMPLETE
Environment-based secret configuration    COMPLETE
DBeaver connectivity                      COMPLETE
retail schema                             COMPLETE
customers table                           COMPLETE
orders table                              COMPLETE
order_items table                         COMPLETE
payments table                            COMPLETE
PK/FK relationships                      COMPLETE
Indexes                                   COMPLETE
Python virtual environment                COMPLETE
Python dependencies                       COMPLETE
Synthetic data generator                  COMPLETE
Small-batch generation                    COMPLETE
Medium-scale generation                   COMPLETE
Foreign-key validation                    COMPLETE
Order-total reconciliation                COMPLETE
Git-safe secret handling                  COMPLETE
```

## 27. Next Phase

The next source system is MySQL for product and inventory data:

```text
MySQL
|- products
|- inventory
|- stores
`- suppliers
```

Cross-system relationship:

```text
PostgreSQL order_items.product_id
              |
              v
MySQL products.product_id
```

After both source systems are complete, the project moves toward Python extraction and AWS S3 ingestion.
