# RetailPulse Enterprise Data Platform
# Phase 4 — Watermark-Based Incremental Ingestion

**Project:** RetailPulse Enterprise Data Platform  
**Feature branch:** `feature/watermark-incremental-ingestion`  
**AWS Region:** `us-east-2`  
**S3 Bucket:** `retailpulse-data-lake-saiteja`  
**DynamoDB Table:** `retailpulse-watermarks`  
**Status:** **COMPLETE — PostgreSQL and MySQL incremental ingestion validated for INSERTs, UPDATEs, S3 delivery, DynamoDB checkpoint advancement, and zero-row reruns.**

---

## 1. Phase 4 Objective

Earlier ingestion logic extracted complete source tables and wrote the result into the S3 raw layer.

Phase 4 changes that design to **stateful incremental ingestion**.

The pipeline now remembers the last successfully processed source position in DynamoDB and extracts only rows that are newer than that checkpoint.

Final logical architecture:

```text
PostgreSQL / MySQL
        |
        v
Python ingestion script
        |
        +---- Read current watermark from DynamoDB
        |
        +---- Query only new / changed source rows
        |
        +---- Create CSV in memory
        |
        +---- Upload incremental batch to S3 raw layer
        |
        `---- Update DynamoDB watermark only after S3 succeeds
```

The final Phase 4 design supports:

```text
PostgreSQL
  INSERT detection  -> YES
  UPDATE detection  -> YES
  Zero-change rerun -> YES

MySQL
  INSERT detection  -> YES
  UPDATE detection  -> YES
  Zero-change rerun -> YES
```

---

# 2. Git Branch and Working Strategy

Phase 4 work was isolated in a feature branch.

Final branch name:

```text
feature/watermark-incremental-ingestion
```

A spelling issue was noticed in the local branch name: the final `n` in `ingestion` was missing.

The current branch was renamed with:

```powershell
git branch -m feature/watermark-incremental-ingestion
```

Verification:

```powershell
git branch --show-current
```

Expected:

```text
feature/watermark-incremental-ingestion
```

No code changes were lost during the branch rename.

Expected Phase 4 files before commit:

```text
src/ingestion/postgres_to_s3.py
src/ingestion/mysql_to_s3.py
sql/postgres/03_add_update_tracking.sql
docs/phase-4-watermark-incremental-ingestion.md
```

Temporary DynamoDB CLI helper files were intentionally removed before commit:

```text
dynamodb-item.json
dynamodb-key.json
```

Removal:

```powershell
Remove-Item .\dynamodb-item.json
Remove-Item .\dynamodb-key.json
```

---

# 3. Why DynamoDB Is Used

Incremental ingestion requires persistent state.

Without a persistent watermark, every run would need to either:

```text
1. reload the entire source table
or
2. rely on a local file that is not centrally managed
```

DynamoDB was selected as the centralized checkpoint store.

Benefits:

```text
AWS-native
centrally available
serverless
low operational overhead
fast key-based reads
easy table-level checkpoint storage
works with boto3
does not depend on one developer laptop
```

The DynamoDB table stores one item for each source table.

---

# 4. DynamoDB Table Configuration

DynamoDB table:

```text
retailpulse-watermarks
```

Region:

```text
us-east-2
```

Partition key:

```text
source_table
```

Partition key type:

```text
String
```

Billing mode:

```text
PAY_PER_REQUEST
```

The table is schema-flexible beyond the partition key.

That is why MySQL and PostgreSQL items can contain attributes such as:

```text
last_id
last_updated_at
```

without defining those attributes as DynamoDB table keys.

The important distinction is:

```text
DynamoDB key schema
    source_table

Item attributes
    last_id
    last_updated_at
```

`last_id` and `last_updated_at` are normal item attributes, not DynamoDB key attributes.

---

# 5. DynamoDB Item Design

Each source table has one DynamoDB item.

PostgreSQL examples:

```text
postgres#customers
postgres#orders
postgres#order_items
postgres#payments
```

MySQL examples:

```text
mysql#suppliers
mysql#products
mysql#stores
mysql#inventory
```

The `source_table` value identifies:

```text
database source + physical table
```

Example:

```text
postgres#customers
```

means:

```text
Source system: PostgreSQL
Table: customers
```

---

# 6. DynamoDB Attribute Types

The final composite watermark contains two values:

```text
last_updated_at
last_id
```

DynamoDB types used:

```text
last_updated_at -> String (S)
last_id         -> Number (N)
```

Example item:

```json
{
  "source_table": {
    "S": "postgres#customers"
  },
  "last_id": {
    "N": "4076"
  },
  "last_updated_at": {
    "S": "2026-08-22 18:17:28.894771"
  }
}
```

Why `last_id` is Number:

```text
It is a numeric primary-key tie-breaker.
```

Why `last_updated_at` is String:

```text
The source timestamp is serialized into a stable timestamp representation
and converted back to a Python datetime when PostgreSQL requires it.
```

---

# 7. How DynamoDB Was Connected to the Local Project

The Python ingestion scripts use the AWS SDK for Python:

```text
boto3
```

No AWS access key or secret key is hard-coded in the source files.

The authentication path is:

```text
Local PowerShell / VS Code
        |
        v
AWS CLI authenticated session
        |
        v
boto3 credential provider chain
        |
        v
DynamoDB client
        |
        v
retailpulse-watermarks
```

## 7.1 Authenticate AWS CLI

When the local AWS session needs authentication:

```powershell
aws login
```

This authenticates the local AWS CLI session.

During Phase 4 an expired session produced:

```text
AccessDeniedException:
The refresh token has expired.

LoginRefreshRequired:
Your session has expired or credentials have changed.
Please reauthenticate using 'aws login'.
```

Fix:

```powershell
aws login
```

No Python change was required.

## 7.2 Verify AWS Identity

Read-only command:

```powershell
aws sts get-caller-identity
```

Purpose:

```text
Confirm that the AWS CLI session is valid
and identify the AWS identity used by boto3.
```

The command successfully returned the authenticated account and IAM identity.

## 7.3 Verify Region

Command:

```powershell
aws configure get region
```

Expected project region:

```text
us-east-2
```

---

# 8. How boto3 Creates the DynamoDB Connection

Both ingestion scripts create one reusable DynamoDB client.

Final pattern:

```python
dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
)
```

Region configuration:

```python
AWS_REGION = os.getenv(
    "AWS_REGION",
    "us-east-2",
)
```

Watermark table configuration:

```python
WATERMARK_TABLE = os.getenv(
    "WATERMARK_TABLE",
    "retailpulse-watermarks",
)
```

This means:

```text
If AWS_REGION exists in the environment -> use it
otherwise -> use us-east-2

If WATERMARK_TABLE exists in the environment -> use it
otherwise -> use retailpulse-watermarks
```

The code does not create a new boto3 client inside every function.

Instead:

```text
one DynamoDB client
one S3 client
reused across the process
```

This is cleaner and avoids unnecessary client construction.

---

# 9. How boto3 Gets Credentials

The Python code intentionally does not contain:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
```

boto3 uses its normal credential provider chain.

In the local development environment, the valid AWS CLI login/session makes credentials available to boto3.

Conceptually:

```text
aws login
    |
    v
authenticated AWS session
    |
    v
boto3 resolves credentials
    |
    v
DynamoDB / S3 API calls
```

For an AWS-hosted production workload, the preferred model would be IAM roles such as:

```text
Lambda execution role
EC2 instance profile
ECS task role
Glue job role
```

instead of developer login credentials.

---

# 10. How Watermarks Are Read from DynamoDB

The scripts use:

```python
response = dynamodb.get_item(
    TableName=WATERMARK_TABLE,
    Key={
        "source_table": {
            "S": source_table
        }
    },
    ConsistentRead=True,
)
```

Example `source_table`:

```python
source_table = f"postgres#{table_name}"
```

or:

```python
source_table = f"mysql#{table_name}"
```

`ConsistentRead=True` is important because the watermark is pipeline state.

The ingestion run should read the latest committed checkpoint rather than an older eventually consistent value.

The scripts fail if the DynamoDB item is missing.

Example:

```python
if not item:
    raise RuntimeError(
        f"No watermark found for {source_table}"
    )
```

They also fail if required attributes are missing.

Example:

```python
if "last_id" not in item:
    raise RuntimeError(
        f"last_id is missing for {source_table}"
    )

if "last_updated_at" not in item:
    raise RuntimeError(
        f"last_updated_at is missing for {source_table}"
    )
```

This is intentional.

The script does **not** silently assume:

```text
last_id = 0
```

because that could cause an accidental complete historical replay.

---

# 11. How Watermarks Are Updated in DynamoDB

After the S3 upload succeeds, the scripts update the DynamoDB item.

Pattern:

```python
dynamodb.update_item(
    TableName=WATERMARK_TABLE,
    Key={
        "source_table": {
            "S": source_table
        }
    },
    UpdateExpression=(
        "SET last_id = :last_id, "
        "last_updated_at = :last_updated_at"
    ),
    ExpressionAttributeValues={
        ":last_id": {
            "N": str(new_id)
        },
        ":last_updated_at": {
            "S": updated_at_string
        },
    },
)
```

Important ordering:

```text
read checkpoint
    |
    v
extract source rows
    |
    v
create CSV
    |
    v
upload to S3
    |
    v
update DynamoDB
```

DynamoDB is **not updated before S3**.

Reason:

```text
If S3 upload fails,
the checkpoint must not move forward.
```

Otherwise the next execution could incorrectly believe that data had already been delivered.

---

# 12. DynamoDB Initial Baseline Configuration

Historical data had already been ingested in earlier project phases.

Therefore Phase 4 did not start from zero.

Initial baseline values were created so the new incremental ingestion would start from the already-processed source position.

Initial PostgreSQL baseline:

```text
postgres#customers
last_id = 2076

postgres#orders
last_id = 4107

postgres#order_items
last_id = 8129

postgres#payments
last_id = 3057
```

Initial MySQL baseline:

```text
mysql#suppliers
last_id         = 20
last_updated_at = 2026-08-20 14:57:10

mysql#stores
last_id         = 20
last_updated_at = 2026-08-20 14:57:10

mysql#products
last_id         = 1100
last_updated_at = 2026-08-20 14:57:10

mysql#inventory
last_id         = 1000
last_updated_at = 2026-08-20 14:57:10
```

The initial items were entered through the DynamoDB AWS Console.

---

# 13. DynamoDB Console Attribute Configuration

For each item, the following values were configured.

Example PostgreSQL item:

```text
source_table
Type: String
Value: postgres#customers
```

```text
last_id
Type: Number
Value: 3076
```

After PostgreSQL was upgraded to composite watermarks:

```text
last_updated_at
Type: String
Value: 2026-08-21 16:55:01.203596
```

Equivalent DynamoDB JSON:

```json
{
  "source_table": {
    "S": "postgres#customers"
  },
  "last_id": {
    "N": "3076"
  },
  "last_updated_at": {
    "S": "2026-08-21 16:55:01.203596"
  }
}
```

The same `last_updated_at` migration timestamp was initially added to:

```text
postgres#customers
postgres#orders
postgres#order_items
postgres#payments
```

with their existing `last_id` values preserved.

---

# 14. DynamoDB CLI JSON Troubleshooting

An earlier attempt was made to use AWS CLI JSON input files.

Temporary files included:

```text
dynamodb-item.json
dynamodb-key.json
```

Example old `dynamodb-item.json`:

```json
{
  "source_table": {
    "S": "postgres#customers"
  },
  "last_id": {
    "N": "2076"
  }
}
```

Example `dynamodb-key.json`:

```json
{
  "source_table": {
    "S": "postgres#customers"
  }
}
```

Problems encountered:

```text
PowerShell inline JSON quoting
UTF-8 BOM in JSON file
```

The BOM issue caused AWS CLI parsing output similar to:

```text
ï»¿{
```

Because the initial baseline was small and only needed one-time setup, the DynamoDB Console was used instead.

The temporary JSON files were later removed from the Git working tree.

---

# 15. Initial PostgreSQL Ingestion Logic — ID-Only Watermark

The first PostgreSQL Phase 4 implementation used only primary keys.

Mapping:

```python
WATERMARK_COLUMNS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}
```

Query pattern:

```sql
SELECT *
FROM retail.<table>
WHERE <primary_key> > %s
ORDER BY <primary_key>;
```

Example:

```sql
SELECT *
FROM retail.customers
WHERE customer_id > 2076
ORDER BY customer_id;
```

This works for inserts because new rows receive higher IDs.

---

# 16. Initial PostgreSQL INSERT Test

A new source batch was generated.

Output:

```text
Customers inserted : 1000
Orders inserted    : 2017
Order items        : 4027
Payments inserted  : 1485
```

Read-only extraction using the ID watermark returned:

```text
customers    1000
orders       2017
order_items  4027
payments     1485
```

Full ingestion successfully uploaded those rows to S3 and advanced DynamoDB.

New ID-only checkpoints:

```text
customers    3076
orders       6124
order_items  12156
payments     4542
```

A second run returned:

```text
customers    0
orders       0
order_items  0
payments     0
```

This proved ID-based incremental INSERT handling.

---

# 17. PostgreSQL UPDATE Limitation

The ID-only solution could not detect updates to existing rows.

Example:

```text
Current customers watermark:
last_id = 3076
```

An existing row:

```text
customer_id = 1
```

was updated.

Read-only extraction returned:

```text
Current watermark for customers: 3076
Rows detected: 0
[]
```

Reason:

```text
customer_id = 1
1 > 3076 -> FALSE
```

The primary key does not change when an existing row is modified.

Therefore:

```text
ID-only PostgreSQL watermark
INSERT -> YES
UPDATE -> NO
```

---

# 18. PostgreSQL Schema Investigation

The existing schema was inspected.

Local schema file:

```text
sql/postgres/01_create_schema.sql
```

Existing timestamp columns:

```text
customers
  created_at
  updated_at

orders
  order_timestamp
  updated_at

order_items
  created_at

payments
  payment_timestamp
```

Only:

```text
customers
orders
```

already had `updated_at`.

`order_items` and `payments` did not.

Trigger inspection returned:

```text
0 rows
```

Therefore even `customers` and `orders` did not automatically refresh `updated_at` during an UPDATE.

Important behavior:

```text
DEFAULT CURRENT_TIMESTAMP
```

handles the insert default.

It does **not** make PostgreSQL behave like MySQL `ON UPDATE CURRENT_TIMESTAMP`.

---

# 19. PostgreSQL Schema Migration Strategy

The original schema file was preserved.

Instead of changing:

```text
sql/postgres/01_create_schema.sql
```

a new migration was created:

```text
sql/postgres/03_add_update_tracking.sql
```

This keeps Git history clear:

```text
01_create_schema.sql
    original operational schema

03_add_update_tracking.sql
    later schema evolution for incremental ingestion
```

---

# 20. Complete PostgreSQL Update-Tracking Migration

Final migration:

```sql
-- ============================================================
-- RetailPulse Enterprise Data Platform
-- PostgreSQL Update Tracking Migration
-- Purpose:
--   Enable INSERT + UPDATE incremental ingestion
--   using (updated_at, primary_key) watermarks
-- ============================================================

BEGIN;


-- ============================================================
-- ORDER ITEMS
-- Add updated_at and preserve historical timestamps
-- ============================================================

ALTER TABLE retail.order_items
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

UPDATE retail.order_items
SET updated_at = created_at
WHERE updated_at IS NULL;

ALTER TABLE retail.order_items
ALTER COLUMN updated_at
SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE retail.order_items
ALTER COLUMN updated_at
SET NOT NULL;


-- ============================================================
-- PAYMENTS
-- Add updated_at and preserve historical timestamps
-- ============================================================

ALTER TABLE retail.payments
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

UPDATE retail.payments
SET updated_at = payment_timestamp
WHERE updated_at IS NULL;

ALTER TABLE retail.payments
ALTER COLUMN updated_at
SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE retail.payments
ALTER COLUMN updated_at
SET NOT NULL;


-- ============================================================
-- Shared trigger function
-- Automatically refresh updated_at whenever a row changes
-- ============================================================

CREATE OR REPLACE FUNCTION retail.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


-- ============================================================
-- CUSTOMERS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_customers_updated_at
ON retail.customers;

CREATE TRIGGER trg_customers_updated_at
BEFORE UPDATE
ON retail.customers
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- ORDERS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_orders_updated_at
ON retail.orders;

CREATE TRIGGER trg_orders_updated_at
BEFORE UPDATE
ON retail.orders
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- ORDER ITEMS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_order_items_updated_at
ON retail.order_items;

CREATE TRIGGER trg_order_items_updated_at
BEFORE UPDATE
ON retail.order_items
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- PAYMENTS TRIGGER
-- ============================================================

DROP TRIGGER IF EXISTS trg_payments_updated_at
ON retail.payments;

CREATE TRIGGER trg_payments_updated_at
BEFORE UPDATE
ON retail.payments
FOR EACH ROW
EXECUTE FUNCTION retail.set_updated_at();


-- ============================================================
-- Incremental ingestion indexes
-- Improve composite watermark query performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_customers_updated_at_id
    ON retail.customers(updated_at, customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at_id
    ON retail.orders(updated_at, order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_updated_at_id
    ON retail.order_items(updated_at, order_item_id);

CREATE INDEX IF NOT EXISTS idx_payments_updated_at_id
    ON retail.payments(updated_at, payment_id);


COMMIT;
```

---

# 21. Why Historical Rows Were Backfilled Carefully

For existing `order_items`:

```sql
UPDATE retail.order_items
SET updated_at = created_at
WHERE updated_at IS NULL;
```

For existing `payments`:

```sql
UPDATE retail.payments
SET updated_at = payment_timestamp
WHERE updated_at IS NULL;
```

This avoids:

```text
old historical row
    |
    + updated_at = current time
    |
    v
pipeline incorrectly treats every old row as newly updated
```

Instead:

```text
historical order item
created_at = historical time
updated_at = same historical time
```

and:

```text
historical payment
payment_timestamp = historical time
updated_at         = same historical time
```

---

# 22. Apply PostgreSQL Migration

Command:

```powershell
Get-Content .\sql\postgres\03_add_update_tracking.sql | docker exec -i retailpulse-postgres psql -v ON_ERROR_STOP=1 -U retailpulse -d retailpulse_orders
```

Explanation:

```text
Get-Content
    read SQL migration from local project

|
    pipe SQL content to Docker

docker exec -i retailpulse-postgres
    execute psql inside PostgreSQL container

-v ON_ERROR_STOP=1
    stop immediately if any SQL statement fails

-U retailpulse
    PostgreSQL user

-d retailpulse_orders
    PostgreSQL database
```

Observed output included:

```text
BEGIN
ALTER TABLE
UPDATE 11865
...
UPDATE 4427
...
CREATE FUNCTION
CREATE TRIGGER
CREATE TRIGGER
CREATE TRIGGER
CREATE TRIGGER
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE INDEX
COMMIT
```

Historical backfill:

```text
order_items = 11865 rows
payments    = 4427 rows
```

---

# 23. Verify PostgreSQL Columns

Verification confirmed all four tables now have:

```text
updated_at
timestamp without time zone
NOT NULL
DEFAULT CURRENT_TIMESTAMP
```

Backfill verification:

```text
order_items
total rows           = 11865
rows with updated_at = 11865
NULL updated_at      = 0

payments
total rows           = 4427
rows with updated_at = 4427
NULL updated_at      = 0
```

---

# 24. Verify PostgreSQL Triggers and Indexes

Triggers:

```text
customers    -> trg_customers_updated_at
orders       -> trg_orders_updated_at
order_items  -> trg_order_items_updated_at
payments     -> trg_payments_updated_at
```

Indexes:

```text
customers    -> idx_customers_updated_at_id
orders       -> idx_orders_updated_at_id
order_items  -> idx_order_items_updated_at_id
payments     -> idx_payments_updated_at_id
```

Composite index shape:

```text
(updated_at, primary_key)
```

This matches the extraction ordering.

---

# 25. PostgreSQL Trigger Test

`order_item_id = 1` was used.

Before:

```text
order_item_id = 1
quantity      = 2
created_at    = 2026-08-12 03:32:54.148241
updated_at    = 2026-08-12 03:32:54.148241
```

Test:

```sql
UPDATE retail.order_items
SET quantity = quantity
WHERE order_item_id = 1
RETURNING order_item_id, quantity, created_at, updated_at;
```

Result:

```text
quantity remained 2
updated_at changed to 2026-08-22 18:00:56.272687
```

This proved:

```text
UPDATE
  |
  v
BEFORE UPDATE trigger
  |
  v
NEW.updated_at = CURRENT_TIMESTAMP
```

---

# 26. PostgreSQL DynamoDB Migration to Composite Watermarks

The currently processed ID values were:

```text
customers    3076
orders       6124
order_items  12156
payments     4542
```

The timestamp corresponding to each already-processed last ID was queried:

```powershell
docker exec -it retailpulse-postgres psql -U retailpulse -d retailpulse_orders -c "SELECT 'customers' AS table_name, customer_id AS last_id, updated_at FROM retail.customers WHERE customer_id = 3076 UNION ALL SELECT 'orders', order_id, updated_at FROM retail.orders WHERE order_id = 6124 UNION ALL SELECT 'order_items', order_item_id, updated_at FROM retail.order_items WHERE order_item_id = 12156 UNION ALL SELECT 'payments', payment_id, updated_at FROM retail.payments WHERE payment_id = 4542;"
```

Result:

```text
customers    3076   2026-08-21 16:55:01.203596
orders       6124   2026-08-21 16:55:01.203596
order_items  12156  2026-08-21 16:55:01.203596
payments     4542   2026-08-21 16:55:01.203596
```

DynamoDB was then updated to:

```text
postgres#customers
last_id         = 3076
last_updated_at = 2026-08-21 16:55:01.203596

postgres#orders
last_id         = 6124
last_updated_at = 2026-08-21 16:55:01.203596

postgres#order_items
last_id         = 12156
last_updated_at = 2026-08-21 16:55:01.203596

postgres#payments
last_id         = 4542
last_updated_at = 2026-08-21 16:55:01.203596
```

This created a clean transition from:

```text
last_id only
```

to:

```text
(last_updated_at, last_id)
```

without replaying the full historical source.

---

# 27. Final PostgreSQL Watermark Configuration in Code

Final explicit configuration:

```python
WATERMARK_COLUMNS = {
    "customers": {
        "updated_at": "updated_at",
        "id": "customer_id",
    },
    "orders": {
        "updated_at": "updated_at",
        "id": "order_id",
    },
    "order_items": {
        "updated_at": "updated_at",
        "id": "order_item_id",
    },
    "payments": {
        "updated_at": "updated_at",
        "id": "payment_id",
    },
}
```

Why explicit mapping:

```text
It makes the two-column watermark visible in configuration
instead of hiding updated_at as an implicit constant.
```

---

# 28. PostgreSQL get_watermark() Logic

Final logical behavior:

```text
1. Build DynamoDB key:
   postgres#<table>

2. Read item with ConsistentRead=True.

3. Validate:
   last_id exists
   last_updated_at exists

4. Convert last_id from DynamoDB Number to Python int.

5. Convert last_updated_at String into Python datetime.

6. Return:
   (last_updated_at, last_id)
```

Core code:

```python
def get_watermark(table_name):

    source_table = f"postgres#{table_name}"

    response = dynamodb.get_item(
        TableName=WATERMARK_TABLE,
        Key={
            "source_table": {
                "S": source_table
            }
        },
        ConsistentRead=True,
    )

    item = response.get("Item")

    if not item:
        raise RuntimeError(
            f"No watermark found for {source_table}"
        )

    if "last_id" not in item:
        raise RuntimeError(
            f"last_id is missing for {source_table}"
        )

    if "last_updated_at" not in item:
        raise RuntimeError(
            f"last_updated_at is missing for {source_table}"
        )

    last_id = int(
        item["last_id"]["N"]
    )

    last_updated_at_string = item[
        "last_updated_at"
    ]["S"]

    try:
        last_updated_at = datetime.strptime(
            last_updated_at_string,
            "%Y-%m-%d %H:%M:%S.%f",
        )

    except ValueError:
        last_updated_at = datetime.strptime(
            last_updated_at_string,
            "%Y-%m-%d %H:%M:%S",
        )

    return last_updated_at, last_id
```

---

# 29. PostgreSQL Incremental SQL Logic

Final query:

```sql
SELECT *
FROM retail.<table>
WHERE updated_at > %s
   OR (
       updated_at = %s
       AND <primary_key> > %s
   )
ORDER BY updated_at, <primary_key>;
```

Example for customers:

```sql
SELECT *
FROM retail.customers
WHERE updated_at > %s
   OR (
       updated_at = %s
       AND customer_id > %s
   )
ORDER BY updated_at, customer_id;
```

The pair:

```text
(updated_at, primary_key)
```

is compared lexicographically.

Example:

```text
Old checkpoint:
(2026-08-21 16:55:01.203596, 3076)

Changed old customer:
(2026-08-22 17:41:06.570394, 1)
```

The second pair is newer even though:

```text
1 < 3076
```

because the timestamp is later.

---

# 30. PostgreSQL get_new_watermark() Logic

The SQL result is ordered by:

```text
updated_at
primary key
```

Therefore the final returned row contains the newest checkpoint.

Core logic:

```python
last_row = rows[-1]

new_updated_at = last_row[
    updated_at_index
]

new_id = last_row[
    id_index
]

return new_updated_at, new_id
```

This becomes the next DynamoDB state.

---

# 31. PostgreSQL S3 Upload Logic

Rows are converted to CSV entirely in memory.

```python
csv_buffer = io.StringIO()
writer = csv.writer(csv_buffer)
writer.writerow(columns)
writer.writerows(rows)
```

No local CSV file is required.

S3 object path:

```text
raw/postgres/<table>/load_date=YYYY-MM-DD/<table>_<UTC_TIMESTAMP>.csv
```

Example:

```text
raw/postgres/customers/load_date=2026-08-22/customers_20260822T181548Z.csv
```

The upload uses:

```python
s3.put_object(
    Bucket=S3_BUCKET,
    Key=s3_key,
    Body=csv_content.encode("utf-8"),
    ContentType="text/csv",
)
```

---

# 32. PostgreSQL Empty-Batch Logic

If no rows are returned:

```python
if not rows:
    print(
        f"No new or updated rows for "
        f"{table_name}. "
        f"Skipping S3 upload."
    )
    continue
```

This prevents:

```text
empty S3 files
unnecessary writes
unnecessary watermark changes
```

---

# 33. PostgreSQL UPDATE Detection Validation

After conversion to the composite watermark, read-only extraction returned:

```text
customers    1
orders       0
order_items  1
payments     0
```

Detected old customer:

```text
customer_id = 1
updated_at  = 2026-08-22 17:41:06.570394
```

Detected old order item:

```text
order_item_id = 1
updated_at    = 2026-08-22 18:00:56.272687
```

Both IDs were far below the previous ID watermark.

Therefore update detection was proven.

Full ingestion:

```text
customers
Rows extracted: 1
New watermark:
2026-08-22 17:41:06.570394 + id 1
S3 upload successful
DynamoDB updated

orders
Rows extracted: 0

order_items
Rows extracted: 1
New watermark:
2026-08-22 18:00:56.272687 + id 1
S3 upload successful
DynamoDB updated

payments
Rows extracted: 0
```

---

# 34. PostgreSQL UPDATE Idempotency Test

The same ingestion was immediately rerun.

Result:

```text
customers    0
orders       0
order_items  0
payments     0
```

This proved the update checkpoints were persisted successfully.

---

# 35. PostgreSQL INSERT Test After Schema Evolution

The existing PostgreSQL data generator was run after adding `updated_at` to `order_items` and `payments`.

Generator output:

```text
Customers inserted : 1000
Orders inserted    : 2041
Order items        : 4141
Payments inserted  : 1517
```

No generator code change was required.

Reason:

```text
order_items.updated_at DEFAULT CURRENT_TIMESTAMP
payments.updated_at    DEFAULT CURRENT_TIMESTAMP
```

The insert statements do not need to manually supply those columns.

Full ingestion extracted exactly:

```text
customers    1000
orders       2041
order_items  4141
payments     1517
```

New checkpoints:

```text
postgres#customers
last_id         = 4076
last_updated_at = 2026-08-22 18:17:28.894771

postgres#orders
last_id         = 8165
last_updated_at = 2026-08-22 18:17:28.894771

postgres#order_items
last_id         = 16297
last_updated_at = 2026-08-22 18:17:28.894771

postgres#payments
last_id         = 6059
last_updated_at = 2026-08-22 18:17:28.894771
```

---

# 36. Final PostgreSQL Zero-Change Rerun

Final validation output:

```text
customers
Rows extracted: 0

orders
Rows extracted: 0

order_items
Rows extracted: 0

payments
Rows extracted: 0
```

Result:

```text
INSERT support       -> PASS
UPDATE support       -> PASS
DynamoDB checkpoint  -> PASS
S3 upload            -> PASS
zero-change rerun    -> PASS
```

---

# 37. MySQL Existing Schema Advantage

MySQL tables already contained update-aware timestamp behavior:

```sql
updated_at TIMESTAMP
DEFAULT CURRENT_TIMESTAMP
ON UPDATE CURRENT_TIMESTAMP
```

Therefore MySQL did not require the PostgreSQL trigger migration.

Final logical watermarks:

```text
suppliers -> updated_at + supplier_id
products  -> updated_at + product_id
stores    -> updated_at + store_id
inventory -> updated_at + inventory_id
```

---

# 38. Final MySQL Watermark Configuration

```python
WATERMARK_ID_COLUMNS = {
    "suppliers": "supplier_id",
    "products": "product_id",
    "stores": "store_id",
    "inventory": "inventory_id",
}
```

All MySQL tables use the same timestamp column:

```text
updated_at
```

Only the ID column differs by table.

---

# 39. MySQL get_watermark() Logic

Core logic:

```python
def get_watermark(table_name):

    source_table = f"mysql#{table_name}"

    response = dynamodb.get_item(
        TableName=WATERMARK_TABLE,
        Key={
            "source_table": {
                "S": source_table
            }
        },
        ConsistentRead=True,
    )

    item = response.get("Item")

    if not item:
        raise RuntimeError(
            f"No watermark found for {source_table}"
        )

    if "last_id" not in item:
        raise RuntimeError(
            f"last_id is missing for {source_table}"
        )

    if "last_updated_at" not in item:
        raise RuntimeError(
            f"last_updated_at is missing for {source_table}"
        )

    last_id = int(
        item["last_id"]["N"]
    )

    last_updated_at = item[
        "last_updated_at"
    ]["S"]

    return last_updated_at, last_id
```

---

# 40. MySQL Incremental Query Logic

Query:

```sql
SELECT *
FROM <table>
WHERE updated_at > %s
   OR (
       updated_at = %s
       AND <primary_key> > %s
   )
ORDER BY updated_at, <primary_key>;
```

Example products query:

```sql
SELECT *
FROM products
WHERE updated_at > %s
   OR (
       updated_at = %s
       AND product_id > %s
   )
ORDER BY updated_at, product_id;
```

This detects:

```text
new rows
+
existing rows whose updated_at changed
```

---

# 41. MySQL Baseline Test

Before generating a new batch:

```text
suppliers 0
stores 0
products 0
inventory 0
```

This proved the initial DynamoDB baseline correctly excluded already-ingested historical rows.

---

# 42. MySQL INSERT Test

New generator batch:

```text
suppliers 10
stores    10
products  50
inventory 500
```

Inventory logic:

```text
50 products
x
10 stores
=
500 inventory rows
```

Read-only extraction detected exactly:

```text
suppliers 10
stores 10
products 50
inventory 500
```

Full ingestion succeeded.

New MySQL checkpoints:

```text
mysql#suppliers
last_id         = 30
last_updated_at = 2026-08-22 15:56:42

mysql#products
last_id         = 1150
last_updated_at = 2026-08-22 15:56:42

mysql#stores
last_id         = 30
last_updated_at = 2026-08-22 15:56:42

mysql#inventory
last_id         = 1500
last_updated_at = 2026-08-22 15:56:43
```

---

# 43. MySQL S3 INSERT Paths

Examples:

```text
raw/mysql/suppliers/load_date=2026-08-22/suppliers_20260822T164947Z.csv

raw/mysql/products/load_date=2026-08-22/products_20260822T164947Z.csv

raw/mysql/stores/load_date=2026-08-22/stores_20260822T164947Z.csv

raw/mysql/inventory/load_date=2026-08-22/inventory_20260822T164948Z.csv
```

---

# 44. MySQL Zero-Change Rerun

Immediately rerunning the ingestion produced:

```text
suppliers 0
products 0
stores 0
inventory 0
```

No new S3 objects were created for empty batches.

---

# 45. MySQL UPDATE Test

An existing supplier was updated:

```text
supplier_id = 1
```

The business column changed:

```text
contact_name = Incremental Update Test
```

Because MySQL uses:

```sql
ON UPDATE CURRENT_TIMESTAMP
```

the row automatically received a newer `updated_at`.

Read-only extraction:

```text
Rows detected: 1
```

Detected row:

```text
supplier_id = 1
updated_at  = 2026-08-22 16:52:38
```

Previous checkpoint:

```text
updated_at = 2026-08-22 15:56:42
id         = 30
```

Even though:

```text
1 < 30
```

the row was detected because its timestamp was newer.

---

# 46. MySQL UPDATE Full Ingestion

Observed:

```text
suppliers
Rows extracted: 1
New watermark:
updated_at=2026-08-22 16:52:38
id=1

products
Rows extracted: 0

stores
Rows extracted: 0

inventory
Rows extracted: 0
```

S3:

```text
raw/mysql/suppliers/load_date=2026-08-22/suppliers_20260822T165510Z.csv
```

New supplier checkpoint:

```text
last_updated_at = 2026-08-22 16:52:38
last_id         = 1
```

This is correct because the real checkpoint is:

```text
(updated_at, id)
```

not `id` alone.

---

# 47. MySQL UPDATE Idempotency Test

Immediate rerun:

```text
suppliers 0
products 0
stores 0
inventory 0
```

Result:

```text
INSERT support       -> PASS
UPDATE support       -> PASS
DynamoDB checkpoint  -> PASS
S3 upload            -> PASS
zero-change rerun    -> PASS
```

---

# 48. MySQL Full Code Logic by Function

The MySQL script follows these functional stages.

## Configuration

```text
Read environment variables
Configure S3 bucket
Configure AWS region
Configure DynamoDB watermark table
Define source tables
Define table-to-ID mapping
```

## AWS Clients

```text
Create reusable DynamoDB client
Create reusable S3 client
```

## get_watermark()

```text
Build mysql#<table> key
Read DynamoDB item
Validate required attributes
Return (last_updated_at, last_id)
```

## extract_table()

```text
Read watermark
Connect to MySQL
Run composite incremental query
Fetch rows
Capture column names
Close connection
Return columns + rows
```

## get_new_watermark()

```text
Rows are already sorted
Take last row
Extract updated_at
Extract table-specific ID
Return new composite checkpoint
```

## create_csv_in_memory()

```text
Create StringIO
Write header
Write rows
Return CSV string
```

## upload_to_s3()

```text
Generate UTC load_date
Generate UTC filename timestamp
Build raw/mysql/<table>/load_date=... path
put_object to S3
Return S3 key
```

## update_watermark()

```text
Serialize updated_at
Update last_id
Update last_updated_at
```

## main()

```text
Loop through all MySQL tables

for each table:
    extract incremental rows

    if no rows:
        skip upload
        do not change watermark

    else:
        calculate new watermark
        create CSV
        upload S3
        update DynamoDB
```

---

# 49. PostgreSQL Full Code Logic by Function

The PostgreSQL script follows the same high-level process with PostgreSQL-specific timestamp handling.

## Configuration

```text
Read PostgreSQL connection environment variables
Configure AWS region
Configure S3 bucket
Configure DynamoDB table
Define four source tables
Define explicit two-column watermark config
```

## get_watermark()

```text
Read postgres#<table> DynamoDB item
Validate last_id
Validate last_updated_at
Convert DynamoDB Number -> Python int
Convert timestamp String -> Python datetime
Return tuple
```

## extract_table()

```text
Resolve updated_at column
Resolve table primary-key column
Read current checkpoint
Connect to PostgreSQL
Execute composite query
Fetch rows and column names
Close cursor + connection
Return rows
```

## get_new_watermark()

```text
Use ordered last row
Extract updated_at
Extract table-specific primary key
Return tuple
```

## create_csv_in_memory()

```text
StringIO
CSV header
CSV rows
return string
```

## upload_to_s3()

```text
Get one UTC current_time
Derive load_date
Derive timestamp
Build raw/postgres path
put_object
```

## update_watermark()

```text
Format PostgreSQL timestamp with microseconds
Write last_id Number
Write last_updated_at String
```

## main()

```text
for each PostgreSQL table:
    extract

    if zero rows:
        skip

    determine newest composite watermark
    serialize batch
    upload S3
    update DynamoDB
```

---

# 50. Why the Primary-Key ID Is Still Required

It may appear that `updated_at` alone should be enough.

It is not always enough.

Multiple rows can share the same timestamp.

Example:

```text
Row A:
updated_at = 10:00:00
id = 10

Row B:
updated_at = 10:00:00
id = 11

Row C:
updated_at = 10:00:00
id = 12
```

If the timestamp alone were stored, the pipeline would not know how far it had progressed inside that timestamp boundary.

Therefore the composite pair is:

```text
(updated_at, id)
```

Query:

```sql
updated_at > last_updated_at

OR

updated_at = last_updated_at
AND id > last_id
```

This creates deterministic ordering.

---

# 51. Why last_id Can Move Backward

After an update to an old row, the `last_id` may decrease.

Example:

```text
Previous:
(2026-08-22 15:56:42, 30)

New:
(2026-08-22 16:52:38, 1)
```

This is valid.

The timestamp is the first comparison value.

So:

```text
16:52:38 > 15:56:42
```

The newer tuple wins regardless of the lower ID.

`last_id` is not an independent high watermark anymore.

It is a tie-breaker inside the timestamp.

---

# 52. S3 Raw Layer Design

PostgreSQL:

```text
raw/postgres/<table>/load_date=YYYY-MM-DD/<table>_<UTC_TIMESTAMP>.csv
```

MySQL:

```text
raw/mysql/<table>/load_date=YYYY-MM-DD/<table>_<UTC_TIMESTAMP>.csv
```

Example:

```text
raw/postgres/orders/load_date=2026-08-22/orders_20260822T181812Z.csv
```

The `load_date` represents ingestion date, not source business date.

This preserves historical raw ingestion batches.

---

# 53. Why CSV Is Created in Memory

Earlier approaches can write a CSV to local disk first.

Phase 4 uses:

```python
io.StringIO()
```

Benefits:

```text
no temporary local files
less local cleanup
direct Python -> S3 flow
simpler ingestion process
works naturally in future serverless/container environments
```

---

# 54. Why S3 Is Updated Before DynamoDB

Correct order:

```text
source extraction
    |
    v
CSV
    |
    v
S3
    |
    v
DynamoDB
```

Incorrect order would be:

```text
DynamoDB checkpoint moves
    |
    v
S3 upload fails
```

That could create data loss because the next execution would skip rows that never reached S3.

The implemented order avoids that failure mode.

---

# 55. Empty Batch Behavior

Both scripts contain explicit zero-row handling.

Expected log:

```text
Rows extracted: 0
No new or updated rows for <table>. Skipping S3 upload.
```

No empty CSV is created.

No S3 request is made.

No DynamoDB update is made.

---

# 56. AWS Login Failure Encountered During Testing

Observed stack trace included:

```text
AccessDeniedException:
The refresh token has expired.
```

and:

```text
LoginRefreshRequired:
Please reauthenticate using 'aws login'.
```

Root cause:

```text
Local AWS authentication session expired.
```

Resolution:

```powershell
aws login
```

Then DynamoDB watermark reads were successful again.

Lesson:

```text
Authentication failures must be separated from application logic failures.
```

No changes to `mysql_to_s3.py` were necessary.

---

# 57. Python Syntax Validation

PostgreSQL:

```powershell
.\.venv\Scripts\python.exe -m py_compile src\ingestion\postgres_to_s3.py
```

MySQL:

```powershell
.\.venv\Scripts\python.exe -m py_compile src\ingestion\mysql_to_s3.py
```

No output means Python syntax is valid.

---

# 58. Git Whitespace Validation

Command:

```powershell
git diff --check
```

Observed warning:

```text
LF will be replaced by CRLF the next time Git touches it
```

This is a Windows Git line-ending warning.

It is not:

```text
a Python syntax error
a failed ingestion test
a whitespace error reported by git diff --check
```

---

# 59. Final DynamoDB State Observed

Final tested checkpoint state:

```text
mysql#inventory
last_id         = 1500
last_updated_at = 2026-08-22 15:56:43

mysql#products
last_id         = 1150
last_updated_at = 2026-08-22 15:56:42

mysql#stores
last_id         = 30
last_updated_at = 2026-08-22 15:56:42

mysql#suppliers
last_id         = 1
last_updated_at = 2026-08-22 16:52:38

postgres#customers
last_id         = 4076
last_updated_at = 2026-08-22 18:17:28.894771

postgres#orders
last_id         = 8165
last_updated_at = 2026-08-22 18:17:28.894771

postgres#order_items
last_id         = 16297
last_updated_at = 2026-08-22 18:17:28.894771

postgres#payments
last_id         = 6059
last_updated_at = 2026-08-22 18:17:28.894771
```

---

# 60. Final Validation Matrix

| Capability | PostgreSQL | MySQL |
|---|---|---|
| Read DynamoDB watermark | PASS | PASS |
| Incremental INSERT detection | PASS | PASS |
| Existing-row UPDATE detection | PASS | PASS |
| Composite `(updated_at, id)` checkpoint | PASS | PASS |
| Create CSV in memory | PASS | PASS |
| Upload incremental batch to S3 | PASS | PASS |
| Advance DynamoDB after S3 | PASS | PASS |
| Skip empty batches | PASS | PASS |
| Zero-change rerun | PASS | PASS |
| Historical data preserved during migration | PASS | N/A |
| Update timestamp automatically maintained | Trigger | MySQL `ON UPDATE` |
| Existing generator remains usable | PASS | PASS |

---

# 61. Production-Hardening Notes

The implementation is correct and tested for the current project.

Additional enterprise improvements can be added later.

## 61.1 Conditional DynamoDB Checkpoint Updates

Current scripts update the watermark after S3 succeeds.

A future improvement can add a DynamoDB:

```text
ConditionExpression
```

to ensure the checkpoint has not changed since the current run read it.

This protects against stale concurrent writers.

## 61.2 Concurrent Job Locking

Conditional updates prevent some stale state changes but do not stop two jobs from uploading overlapping S3 batches.

Possible future solutions:

```text
DynamoDB lock / lease
Step Functions orchestration
single-execution scheduler
idempotent batch IDs
```

## 61.3 MySQL Same-Second Timestamp Boundary

MySQL timestamps observed in this project use second-level precision.

A rare high-concurrency edge case can occur if:

```text
stored checkpoint timestamp = T
stored id = 30

old row id = 5
gets updated in exact same second T
```

It could fail:

```text
updated_at > T        -> false
updated_at = T        -> true
id > 30               -> false
```

Production options:

```text
TIMESTAMP(6)
lookback + deduplication
binlog CDC
AWS DMS
Debezium
```

## 61.4 Large Batches

Current code uses:

```text
fetchall()
StringIO
```

which is appropriate for the project scale.

At larger scale consider:

```text
server-side cursors
chunked fetch
multipart S3 upload
Parquet
AWS DMS
Glue-native ingestion
```

---

# 62. Final End-to-End Architecture

```text
                       +----------------------+
                       |      PostgreSQL      |
                       |----------------------|
                       | customers            |
                       | orders               |
                       | order_items          |
                       | payments             |
                       +----------+-----------+
                                  |
                                  v
                         postgres_to_s3.py
                                  |
                +-----------------+-----------------+
                |                                   |
                v                                   v
        DynamoDB watermark                     S3 raw
       (updated_at, id)                 raw/postgres/...
                |
                v
         next ingestion run


                       +----------------------+
                       |        MySQL         |
                       |----------------------|
                       | suppliers            |
                       | products             |
                       | stores               |
                       | inventory            |
                       +----------+-----------+
                                  |
                                  v
                           mysql_to_s3.py
                                  |
                +-----------------+-----------------+
                |                                   |
                v                                   v
        DynamoDB watermark                     S3 raw
       (updated_at, id)                   raw/mysql/...
                |
                v
         next ingestion run
```

Downstream architecture remains:

```text
S3 raw
   |
   v
Glue crawler / workflow
   |
   v
standardization
   |
   v
S3 standardized Parquet
   |
   v
Athena
```

---

# 63. Phase 4 Completion Summary

Phase 4 introduced centralized, persistent, watermark-based incremental ingestion.

DynamoDB responsibilities:

```text
Store one checkpoint per source table
Store primary-key tie-breaker
Store last updated timestamp
Provide state to the next run
Advance only after S3 succeeds
```

PostgreSQL required additional schema work:

```text
Add updated_at to order_items
Add updated_at to payments
Backfill historical timestamps safely
Add update triggers to all four tables
Add composite indexes
Convert DynamoDB state from ID-only to timestamp + ID
Convert Python extraction from ID-only to composite watermark
```

MySQL already had automatic update timestamps:

```text
DEFAULT CURRENT_TIMESTAMP
ON UPDATE CURRENT_TIMESTAMP
```

so only the Python ingestion and DynamoDB watermark logic were required.

The final result was tested with:

```text
baseline with zero new rows
new INSERT batches
updates to existing low-ID rows
automatic timestamp tracking
incremental S3 delivery
DynamoDB advancement
immediate reruns returning zero rows
```

**Phase 4 is functionally complete and ready for final Git review, staging, commit, push, pull request, and merge.**
