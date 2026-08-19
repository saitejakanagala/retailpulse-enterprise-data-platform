# RetailPulse Enterprise Data Platform
## Phase 3 — AWS Glue Catalog, Athena Validation, and Standardized Parquet Layer

**Project:** RetailPulse — Enterprise AWS + Snowflake Data Engineering Platform  
**Phase:** 3  
**Region:** `us-east-2` (US East — Ohio)  
**Status:** Manual standardization completed and validated for all 8 PostgreSQL/MySQL source tables  
**Date:** 2026-08-18

---

## 1. Phase 3 Objective

The goal of Phase 3 was to take the raw source-system extracts already stored in Amazon S3 and build a standardized analytics-ready layer.

The implemented flow is:

```text
PostgreSQL / MySQL source systems
        ↓
Python ingestion
        ↓
S3 RAW CSV layer
        ↓
AWS Glue Raw Crawler
        ↓
Glue Data Catalog — retailpulse_raw
        ↓
AWS Glue PySpark standardization jobs
        ↓
S3 STANDARDIZED Parquet layer
        ↓
AWS Glue Standardized Crawler
        ↓
Glue Data Catalog — retailpulse_standardized
        ↓
Amazon Athena validation / business queries
```

The standardized layer now contains clean, typed, partitioned Parquet datasets for all eight source tables.

---

## 2. AWS Region Validation

The project S3 bucket region was verified with AWS CLI:

```powershell
aws s3api get-bucket-location --bucket retailpulse-data-lake-saiteja
```

Result:

```text
LocationConstraint: us-east-2
```

Therefore, all Glue and Athena resources for this phase were configured in:

```text
us-east-2
```

---

## 3. IAM Role for AWS Glue

A dedicated AWS Glue service role was created:

```text
AWSGlueServiceRole-RetailPulse
```

### Trust relationship

The role trusts the AWS Glue service:

```json
{
  "Effect": "Allow",
  "Principal": {
    "Service": "glue.amazonaws.com"
  },
  "Action": "sts:AssumeRole"
}
```

### Managed policy attached

```text
AWSGlueServiceRole
```

### Custom S3 policy

A custom policy was created:

```text
RetailPulseGlueS3Access
```

Policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::retailpulse-data-lake-saiteja/*",
        "arn:aws:s3:::retailpulse-data-lake-saiteja"
      ]
    }
  ]
}
```

### Why the role is required

AWS Glue jobs need permission to read raw files, write standardized files, use temporary/script locations, access Catalog metadata, and emit logs/metrics.

A future hardening step is to tighten the custom S3 policy so Glue has read-only access to `raw/` and write/delete access only to the required standardized/temp prefixes.

---

## 4. Raw Glue Data Catalog

A Glue database was created:

```text
retailpulse_raw
```

A crawler was configured against:

```text
s3://retailpulse-data-lake-saiteja/raw/
```

The crawler completed successfully and discovered eight source tables:

```text
customers
orders
order_items
payments
products
suppliers
stores
inventory
```

Crawler result:

```text
8 table changes
9 partition changes
```

The extra partition is expected because `customers` had two load dates while the other tables initially had one.

---

## 5. Athena Query Results Location

Athena query output was configured to:

```text
s3://retailpulse-data-lake-saiteja/athena-results/
```

Athena uses the Glue Data Catalog as table metadata and reads the actual data from S3.

---

## 6. Raw-Layer Athena Validation

A cross-source Athena join was successfully executed across data originally sourced from PostgreSQL and MySQL:

```text
customers
   ↓
orders
   ↓
order_items
   ↓
products
```

This validated that raw CSV files were readable from S3, Catalog schemas were correct, cross-source joins worked, logical relationships were valid, and partition pruning could be applied using `load_date`.

---

## 7. Standardized Layer Design

A second Glue database was created:

```text
retailpulse_standardized
```

The standardized S3 design is:

```text
s3://retailpulse-data-lake-saiteja/standardized/

  postgres/
    customers/
    orders/
    order_items/
    payments/

  mysql/
    products/
    suppliers/
    stores/
    inventory/
```

Each table is written as Parquet and partitioned by `load_date`.

Example:

```text
standardized/mysql/products/
    load_date=2026-08-14/
        part-....snappy.parquet
```

Spark-generated `part-...snappy.parquet` file names were intentionally retained because they are normal for distributed Spark output.

---

## 8. Important Standardized Crawler Path Issue and Fix

### Problem

The standardized crawler was initially pointed too high in the hierarchy:

```text
s3://retailpulse-data-lake-saiteja/standardized/
```

Glue interpreted intermediate folders as unnamed partitions and produced metadata such as:

```text
partition_0 = postgres
partition_1 = customers
load_date   = 2026-08-14
```

It also created an incorrect generalized table such as `standardized`.

### Manual schema-edit attempt

An attempt to remove the bad partition columns directly failed with:

```text
PartitionColumns cannot be deleted when indexes are enabled on the table.
```

### Correct fix

The incorrect Catalog metadata table was deleted and the crawler source was changed to the real table root:

```text
s3://retailpulse-data-lake-saiteja/standardized/postgres/customers/
```

The crawler was run again and produced the clean table:

```text
retailpulse_standardized.customers
```

with only `load_date` as the partition column.

### Rule adopted

Use explicit table-root prefixes:

```text
s3://retailpulse-data-lake-saiteja/standardized/postgres/customers/
s3://retailpulse-data-lake-saiteja/standardized/postgres/orders/
s3://retailpulse-data-lake-saiteja/standardized/postgres/order_items/
s3://retailpulse-data-lake-saiteja/standardized/postgres/payments/

s3://retailpulse-data-lake-saiteja/standardized/mysql/products/
s3://retailpulse-data-lake-saiteja/standardized/mysql/suppliers/
s3://retailpulse-data-lake-saiteja/standardized/mysql/stores/
s3://retailpulse-data-lake-saiteja/standardized/mysql/inventory/
```

---

## 9. Glue PySpark Job Configuration

Initial jobs used:

```text
Glue version:          5.1
Job type:              Spark
Language:              Python 3
Worker type:           G.1X
Number of workers:     2
Retries:               0
Max concurrency:       1
Timeout:               10 minutes
Job bookmarks:         Disabled for initial full-load testing
Execution class:       Standard
IAM role:              AWSGlueServiceRole-RetailPulse
```

Script location:

```text
s3://retailpulse-data-lake-saiteja/glue/scripts/
```

Temporary location:

```text
s3://retailpulse-data-lake-saiteja/glue/temp/
```

Monitoring enabled:

```text
Job metrics
Observability metrics
Continuous CloudWatch logging
Spark UI
```

---

## 10. DynamicFrame vs Spark DataFrame

Glue reads Catalog data into a `DynamicFrame`:

```python
suppliers_dynamic_frame = (
    glueContext.create_dynamic_frame.from_catalog(
        database="retailpulse_raw",
        table_name="suppliers",
        push_down_predicate="load_date='2026-08-14'"
    )
)
```

Conceptually:

```text
Glue Data Catalog
      ↓
retailpulse_raw.suppliers
      ↓
AWS Glue DynamicFrame
```

The DynamicFrame is then converted to a Spark DataFrame:

```python
suppliers_df = suppliers_dynamic_frame.toDF()
```

This is done because PySpark transformations such as `filter()`, `dropDuplicates()`, `withColumn()`, `trim()`, `upper()`, `lower()`, `to_timestamp()`, and `to_date()` are naturally performed on Spark DataFrames.

Full flow:

```text
S3 files
  ↓
Glue Catalog
  ↓
DynamicFrame
  ↓
Spark DataFrame
  ↓
PySpark transformations
  ↓
Parquet
```

---

## 11. Partition Pushdown

Current standardization jobs use:

```python
push_down_predicate="load_date='2026-08-14'"
```

This causes Glue to read only the required partition instead of scanning all dates.

Benefits:

```text
less S3 scanning
faster processing
lower cost
better scalability
```

The fixed date is suitable for the current test and will later be replaced with incremental/watermark-driven processing.

---

## 12. PostgreSQL Standardization

The standardized PostgreSQL layer contains:

```text
customers       COMPLETE
orders          COMPLETE
order_items     COMPLETE
payments        COMPLETE
```

### Customers

Job:

```text
retailpulse-standardize-customers
```

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/postgres/customers/
```

Transformations:

```text
customer_id not NULL
deduplicate by customer_id
created_at → timestamp
updated_at → timestamp
add standardized_at
write Parquet
partition by load_date
```

#### Error 1 — missing `col`

Initial run failed with:

```text
INVALID_ARGUMENT_ERROR
NameError: name 'col' is not defined
```

Fix:

```python
from pyspark.sql.functions import col, to_timestamp, current_timestamp
```

#### Error 2 — timestamp microseconds

Next run failed because raw values such as:

```text
2026-08-12 03:32:54.148241
```

contained microseconds while an explicit whole-second format was being used.

Fix:

```python
to_timestamp(col("created_at"))
to_timestamp(col("updated_at"))
```

The job then succeeded.

### Orders

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/postgres/orders/
```

Transformations:

```text
order_id not NULL
customer_id not NULL
order_total NULL or >= 0
deduplicate by order_id
order_status → trim + uppercase
order_channel → trim + uppercase
order_timestamp → timestamp
updated_at → timestamp
add standardized_at
partition by load_date
```

### Order Items

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/postgres/order_items/
```

Transformations:

```text
order_item_id not NULL
order_id not NULL
product_id not NULL
quantity > 0
unit_price NULL or >= 0
deduplicate by order_item_id
created_at → timestamp
add standardized_at
partition by load_date
```

### Payments

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/postgres/payments/
```

Transformations used/planned for the completed standardized table:

```text
payment_id not NULL
order_id not NULL
amount NULL or >= 0
deduplicate by payment_id
payment_method → trim + uppercase
payment_status → trim + uppercase
transaction_reference → trim
amount → decimal(12,2)
payment_timestamp → timestamp
add standardized_at
partition by load_date
```

---

## 13. MySQL Standardization

All four MySQL jobs were executed manually and validated through Athena.

### 13.1 Products

Job:

```text
retailpulse-standardize-products
```

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/mysql/products/
```

Raw schema:

```text
product_id           bigint
supplier_id          bigint
product_name         string
category             string
brand                string
unit_cost            double
unit_price           double
product_status       string
created_at           string
updated_at           string
load_date            string
```

Transformations:

```text
product_id not NULL
supplier_id not NULL
unit_cost NULL or >= 0
unit_price NULL or >= 0
deduplicate by product_id
product_name → trim
brand → trim
category → trim + uppercase
product_status → trim + uppercase
unit_cost → decimal(12,2)
unit_price → decimal(12,2)
created_at → timestamp
updated_at → timestamp
add standardized_at
partition by load_date
```

Standardized schema validated in Athena:

```text
product_id           bigint
supplier_id          bigint
product_name         string
category             string
brand                string
unit_cost            decimal(12,2)
unit_price            decimal(12,2)
product_status       string
created_at           timestamp
updated_at           timestamp
standardized_at      timestamp
load_date            string
```

Status: **COMPLETE**

---

### 13.2 Suppliers

Job:

```text
retailpulse-standardize-suppliers
```

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/mysql/suppliers/
```

Raw schema:

```text
supplier_id
supplier_name
contact_name
email
phone
city
state
country
created_at
updated_at
load_date
```

Transformations:

```text
supplier_id not NULL
deduplicate by supplier_id
supplier_name → trim
contact_name → trim
email → trim + lowercase
phone → trim
city → trim
state → trim + uppercase
country → trim + uppercase
created_at → timestamp
updated_at → timestamp
add standardized_at
partition by load_date
```

Phone numbers remain in mixed source formats because the current rule only trims the field. More advanced phone normalization is deferred to a later data-quality phase.

Status: **COMPLETE**

---

### 13.3 Stores

Job:

```text
retailpulse-standardize-stores
```

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/mysql/stores/
```

Raw schema:

```text
store_id
store_name
store_type
city
state
country
opened_date
created_at
updated_at
load_date
```

Transformations:

```text
store_id not NULL
deduplicate by store_id
store_name → trim
store_type → trim + uppercase
city → trim
state → trim + uppercase
country → trim + uppercase
opened_date → date
created_at → timestamp
updated_at → timestamp
add standardized_at
partition by load_date
```

`to_date()` was used for `opened_date` because it represents a calendar date rather than a point in time.

Status: **COMPLETE**

---

### 13.4 Inventory

Job:

```text
retailpulse-standardize-inventory
```

Output:

```text
s3://retailpulse-data-lake-saiteja/standardized/mysql/inventory/
```

Raw schema:

```text
inventory_id         bigint
product_id           bigint
store_id             bigint
quantity_on_hand     bigint
reorder_level        bigint
last_restocked_at    string
updated_at           string
load_date            string
```

Transformations:

```text
inventory_id not NULL
product_id not NULL
store_id not NULL
quantity_on_hand NULL or >= 0
reorder_level NULL or >= 0
deduplicate by inventory_id
last_restocked_at → timestamp
updated_at → timestamp
add standardized_at
partition by load_date
```

Standardized schema validated:

```text
inventory_id         bigint
product_id           bigint
store_id             bigint
quantity_on_hand     bigint
reorder_level        bigint
last_restocked_at    timestamp
updated_at           timestamp
standardized_at      timestamp
load_date            string
```

Status: **COMPLETE**

---

## 14. Standardized MySQL Business Join

A business validation query was executed across:

```text
inventory
products
stores
```

Relationships:

```text
inventory.product_id → products.product_id
inventory.store_id   → stores.store_id
```

Query:

```sql
SELECT
    i.inventory_id,
    p.product_name,
    s.store_name,
    i.quantity_on_hand,
    i.reorder_level,
    CASE
        WHEN i.quantity_on_hand <= i.reorder_level
        THEN 'REORDER'
        ELSE 'OK'
    END AS inventory_status
FROM retailpulse_standardized.inventory i
JOIN retailpulse_standardized.products p
    ON i.product_id = p.product_id
JOIN retailpulse_standardized.stores s
    ON i.store_id = s.store_id
WHERE i.load_date = '2026-08-14'
  AND p.load_date = '2026-08-14'
  AND s.load_date = '2026-08-14'
ORDER BY i.quantity_on_hand
LIMIT 20;
```

The query successfully returned product names, store names, quantities, reorder levels, and a derived inventory status.

Example:

```text
Inventory ID:     368
Product:          Triple-buffered tangible paradigm
Store:            RetailPulse Cortezview
Quantity on hand: 0
Reorder level:    45
Status:            REORDER
```

Rule:

```text
quantity_on_hand <= reorder_level
    → REORDER
otherwise
    → OK
```

---

## 15. Inventory Replenishment KPI Validation

Summary query:

```sql
SELECT
    CASE
        WHEN quantity_on_hand <= reorder_level
        THEN 'REORDER'
        ELSE 'OK'
    END AS inventory_status,
    COUNT(*) AS record_count
FROM retailpulse_standardized.inventory
WHERE load_date = '2026-08-14'
GROUP BY
    CASE
        WHEN quantity_on_hand <= reorder_level
        THEN 'REORDER'
        ELSE 'OK'
    END
ORDER BY inventory_status;
```

Result:

```text
OK        456
REORDER    44
```

Total inventory records:

```text
500
```

Percentages:

```text
OK       = 91.2%
REORDER  =  8.8%
```

Business interpretation:

```text
456 inventory records currently have stock above the reorder threshold.
44 inventory records require replenishment based on the current business rule.
```

This is the first standardized-layer inventory replenishment KPI produced from the MySQL data.

---

## 16. Completed Standardized Tables

### PostgreSQL

```text
customers       COMPLETE
orders          COMPLETE
order_items     COMPLETE
payments        COMPLETE
```

### MySQL

```text
products        COMPLETE
suppliers       COMPLETE
stores          COMPLETE
inventory       COMPLETE
```

Total standardized tables: **8**

---

## 17. Current End-to-End Validation Status

```text
Local PostgreSQL / MySQL
        ↓
Python extraction
        ↓
CSV
        ↓
S3 raw/
        ↓
Glue Raw Crawler
        ↓
retailpulse_raw
        ↓
Glue PySpark jobs
        ↓
Parquet
        ↓
S3 standardized/
        ↓
Standardized Crawler
        ↓
retailpulse_standardized
        ↓
Athena
        ↓
Cross-table business joins
        ↓
Inventory replenishment KPI
```

Status: **PASS**

---

## 18. Manual Execution Approach Used

For learning and validation, all standardization jobs were finished manually before orchestration was introduced.

```text
Create / run Glue job
        ↓
Confirm job = SUCCEEDED
        ↓
Confirm Parquet output in S3
        ↓
Run retailpulse-standardized-crawler manually
        ↓
Confirm Glue Catalog table/schema
        ↓
Query in Athena
```

This was intentional so each transformation could be tested and debugged independently.

---

## 19. AWS Glue Workflow Automation — Implemented and Validated

After all eight standardization jobs were successfully tested manually, orchestration was added using an AWS Glue Workflow.

Workflow:

```text
retailpulse-standardization-workflow
```

### Start trigger

```text
retailpulse-standardization-start
```

Type:

```text
On-demand
```

The start trigger launches all eight standardization jobs:

```text
PostgreSQL
- retailpulse-standardize-customers
- retailpulse-standardize-orders
- retailpulse-standardize-order-items
- retailpulse-standardize-payments

MySQL
- retailpulse-standardize-products
- retailpulse-standardize-suppliers
- retailpulse-standardize-stores
- retailpulse-standardize-inventory
```

### Conditional success trigger

```text
retailpulse-standardized-crawler-on-success
```

Configuration:

```text
Trigger type: CONDITIONAL
Trigger state: ACTIVATED
Trigger logic: Start after ALL watched events
```

The trigger watches all eight standardization jobs for:

```text
SUCCEEDED
```

Only after all eight jobs succeed does it start:

```text
retailpulse-standardized-crawler
```

Final automated flow:

```text
retailpulse-standardization-start
              ↓
       8 Glue PySpark jobs
              ↓
       ALL 8 SUCCEEDED
              ↓
retailpulse-standardized-crawler-on-success
              ↓
retailpulse-standardized-crawler
              ↓
Glue Data Catalog updated
              ↓
Athena
```

### Successful workflow run

The workflow was executed successfully on August 18, 2026.

Observed run:

```text
Status:       Completed
Duration:     03 min 03 sec
Started:      Aug 18, 2026 23:38:38
Completed:    Aug 18, 2026 23:41:41
```

The workflow graph showed successful completion of the start trigger, all eight standardization jobs, the ALL-success conditional trigger, and the standardized crawler.

This confirms that the crawler now runs automatically after the complete standardization batch succeeds.

### Workflow execution rule

When validating this dependency chain, the eight jobs should be started through the workflow/start trigger rather than as unrelated individual manual runs. This keeps the watched job events inside the workflow dependency chain.

---

## 20. Post-Workflow Data Validation

After the automated workflow completed, Athena was used to verify that the standardized output was actually regenerated.

### Inventory row-count check

```sql
SELECT
    COUNT(*) AS inventory_count
FROM retailpulse_standardized.inventory
WHERE load_date = '2026-08-14';
```

Result:

```text
inventory_count = 500
```

This confirmed that all expected standardized inventory records remained available after the automated workflow.

### Standardization timestamp check

To prove that the Glue standardization job reprocessed the data, the following query was executed:

```sql
SELECT
    MIN(standardized_at) AS earliest_standardized_at,
    MAX(standardized_at) AS latest_standardized_at,
    COUNT(*) AS record_count
FROM retailpulse_standardized.inventory
WHERE load_date = '2026-08-14';
```

Result:

```text
earliest_standardized_at = 2026-08-18 23:40:46.882
latest_standardized_at   = 2026-08-18 23:40:46.882
record_count             = 500
```

Interpretation:

```text
all 500 rows were processed in the automated batch
all 500 rows received the new standardized_at timestamp
standardized Parquet output was regenerated
the crawler refreshed the Glue Catalog
Athena successfully read the refreshed standardized data
```

This validates the workflow at both the orchestration level and the data level.

---

## 21. Future Improvements

The current jobs use full-load overwrite logic:

```python
.write.mode("overwrite")
```

and a fixed partition:

```python
push_down_predicate="load_date='2026-08-14'"
```

Next enterprise improvements:

```text
dynamic/incremental load_date handling
job bookmarks or watermarks
idempotent reruns
MERGE/upsert patterns
late-arriving data handling
schema evolution
data-quality rules
quarantine/reprocessing
record-count reconciliation
CloudWatch alarms / SNS notifications
least-privilege IAM hardening
```

---

## 22. Key Lessons from Phase 3

1. Glue crawlers discover S3 schemas and register metadata in the Glue Data Catalog.
2. Athena queries S3 using Catalog metadata.
3. Glue DynamicFrames are ETL-friendly Glue structures; Spark DataFrames are used for PySpark transformations.
4. `push_down_predicate` reads only the required partition.
5. Timestamp parsing should not use an overly restrictive format when microseconds may be present.
6. PySpark functions such as `col()` must be imported explicitly.
7. Standardized crawler paths should point to table roots, not overly broad prefixes.
8. Spark normally writes `part-....snappy.parquet` files.
9. `load_date` is the current batch partitioning strategy.
10. Manual validation before orchestration makes debugging easier.
11. A Glue Workflow can launch all eight standardization jobs and run the standardized crawler only after every watched job succeeds.
12. The automated workflow completed successfully in 3 minutes 3 seconds.
13. The inventory business join proved that standardized tables support real analytics.
14. The replenishment KPI showed 44 of 500 inventory records require reorder action under the current rule.
15. `standardized_at` provides a useful batch audit field; the automated rerun refreshed all 500 inventory rows to `2026-08-18 23:40:46.882`.

---

## 23. Phase 3 Checkpoint

```text
Raw S3 landing              COMPLETE
Raw Glue Catalog            COMPLETE
Athena raw validation       COMPLETE
PostgreSQL standardization  COMPLETE
MySQL standardization       COMPLETE
Standardized Parquet        COMPLETE
Standardized Catalog        COMPLETE
Athena standardized joins   COMPLETE
Inventory KPI validation    COMPLETE
Glue Workflow automation    COMPLETE
Conditional trigger         COMPLETE
Automatic crawler run       COMPLETE
Post-workflow Athena check  COMPLETE
```

**Phase 3 standardization and Glue workflow automation checkpoint is successfully complete.**

**Next engineering step:** replace the hard-coded `load_date='2026-08-14'` processing logic with a dynamic/incremental load strategy.
