# RetailPulse Enterprise Data Platform
## Phase 3 — AWS Glue Catalog, Athena Validation, Standardized Parquet Layer, and Workflow Orchestration

**Project:** RetailPulse — Enterprise AWS + Snowflake Data Engineering Platform  
**Phase:** 3  
**Region:** `us-east-2` (US East — Ohio)  
**Status:** COMPLETE — multi-batch standardization, dynamic `load_date`, workflow orchestration, historical partition preservation, and Athena validation completed
**Updated:** 2026-08-20

---

## 1. Phase 3 Objective

Phase 3 converted raw source extracts in Amazon S3 into a standardized analytics-ready Parquet layer using AWS Glue and Amazon Athena.

The phase began with manual, single-batch validation and evolved into a reusable multi-batch workflow.

Final implemented flow:

```text
PostgreSQL / MySQL source systems
        ↓
Python extraction
        ↓
CSV generated in Python memory
        ↓
boto3 put_object()
        ↓
S3 RAW CSV layer
        ↓
AWS Glue Workflow
        ↓
Raw Crawler
        ↓
Glue Data Catalog — retailpulse_raw
        ↓
8 AWS Glue PySpark standardization jobs
        ↓
S3 STANDARDIZED Parquet layer
        ↓
Standardized Crawler
        ↓
Glue Data Catalog — retailpulse_standardized
        ↓
Amazon Athena validation / business queries
```

The standardized layer now contains clean, typed, partitioned Parquet datasets for all eight source tables and preserves multiple `load_date` partitions.

---

## 2. AWS Region and IAM

Bucket region was verified using:

```powershell
aws s3api get-bucket-location --bucket retailpulse-data-lake-saiteja
```

Result:

```text
LocationConstraint: us-east-2
```

Glue service role:

```text
AWSGlueServiceRole-RetailPulse
```

Managed policy:

```text
AWSGlueServiceRole
```

Custom S3 policy:

```text
RetailPulseGlueS3Access
```

Current S3 permissions include:

```text
s3:GetObject
s3:PutObject
s3:DeleteObject
s3:ListBucket
```

Future hardening should scope these permissions to only the required prefixes.

---

## 3. Raw Glue Data Catalog

Glue database:

```text
retailpulse_raw
```

Crawler:

```text
retailpulse-raw-crawler
```

Raw root:

```text
s3://retailpulse-data-lake-saiteja/raw/
```

Discovered tables:

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

The raw crawler is now part of the Glue Workflow and does not need to be run manually during a normal workflow execution.

---

## 4. Athena Query Results

Athena result location:

```text
s3://retailpulse-data-lake-saiteja/athena-results/
```

A raw cross-source join was validated across:

```text
customers
   ↓
orders
   ↓
order_items
   ↓
products
```

This proved that:

```text
raw CSV files are readable from S3
Glue Catalog schemas are usable
cross-source joins work
logical product relationships work
load_date partition filtering works
```

---

## 5. Standardized Layer

Glue database:

```text
retailpulse_standardized
```

S3 layout:

```text
standardized/
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

Each table is written as Parquet and partitioned by:

```text
load_date
```

Example:

```text
standardized/postgres/customers/
├── load_date=2026-08-14/
└── load_date=2026-08-20/
```

Spark-generated `part-....snappy.parquet` files were retained because this is normal distributed Spark output.

---

## 6. Standardized Crawler Path Issue and Fix

### Problem

The standardized crawler was initially pointed to:

```text
s3://retailpulse-data-lake-saiteja/standardized/
```

Glue interpreted intermediate folders as partitions and produced metadata such as:

```text
partition_0 = postgres
partition_1 = customers
load_date   = 2026-08-14
```

It also created an incorrect generalized table.

Attempting to manually remove the incorrect partition columns failed with:

```text
PartitionColumns cannot be deleted when indexes are enabled on the table.
```

### Fix

The incorrect Catalog metadata was deleted and the crawler was changed to explicit table roots:

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

This produced clean Catalog tables with only `load_date` as the partition column.

---

## 7. Glue PySpark Job Configuration

```text
Glue version:          5.1
Job type:              Spark
Language:              Python 3
Worker type:           G.1X
Number of workers:     2
Retries:               0
Max concurrency:       1
Timeout:               10 minutes
Job bookmarks:         Disabled
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

## 8. DynamicFrame and Spark DataFrame

Glue reads Catalog data into a DynamicFrame.

Example:

```python
customers_dynamic_frame = (
    glueContext.create_dynamic_frame.from_catalog(
        database="retailpulse_raw",
        table_name="customers",
        push_down_predicate=f"load_date='{load_date}'"
    )
)
```

The DynamicFrame is converted to a Spark DataFrame:

```python
customers_df = customers_dynamic_frame.toDF()
```

Flow:

```text
S3
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

## 9. Dynamic Workflow `load_date`

The initial jobs used a fixed partition:

```python
push_down_predicate="load_date='2026-08-14'"
```

This was replaced with a workflow run property.

Workflow property:

```text
Key:   load_date
Value: YYYY-MM-DD
```

Example:

```text
load_date = 2026-08-20
```

Each Glue job receives:

```python
JOB_NAME
WORKFLOW_NAME
WORKFLOW_RUN_ID
```

and reads the workflow property:

```python
import boto3

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID']
)

glue_client = boto3.client("glue")

workflow_properties = glue_client.get_workflow_run_properties(
    Name=args['WORKFLOW_NAME'],
    RunId=args['WORKFLOW_RUN_ID']
)["RunProperties"]

if "load_date" not in workflow_properties:
    raise Exception("Workflow property 'load_date' is missing")

load_date = workflow_properties["load_date"]

print(f"Processing load_date: {load_date}")
```

Partition read:

```python
push_down_predicate=f"load_date='{load_date}'"
```

Benefits:

```text
no hard-coded processing date
one reusable job definition
partition pruning
lower scan volume
workflow-controlled batch execution
```

---

## 10. Parameterization Errors and Fixes

### Missing f-string

Incorrect:

```python
push_down_predicate="load_date='{load_date}'"
```

Correct:

```python
push_down_predicate=f"load_date='{load_date}'"
```

### Missing GlueContext initialization

Correct initialization:

```python
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
```

Both issues were corrected before workflow validation.

---

## 11. Empty-Input Protection

All eight jobs were updated to fail when the requested raw partition is empty.

Example:

```python
inventory_df = inventory_dynamic_frame.toDF()

if inventory_df.rdd.isEmpty():
    raise Exception(
        f"No raw inventory data found for load_date={load_date}"
    )
```

Equivalent checks were added for:

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

This prevents a missing input partition from being silently treated as a successful run.

---

## 12. PostgreSQL Standardization

Completed tables:

```text
customers
orders
order_items
payments
```

### Customers

Transformations:

```text
customer_id not NULL
deduplicate by customer_id
created_at → timestamp
updated_at → timestamp
add standardized_at
partition by load_date
```

Initial error:

```text
NameError: name 'col' is not defined
```

Fix:

```python
from pyspark.sql.functions import col, to_timestamp, current_timestamp
```

Timestamp parsing also initially failed because raw timestamps contained microseconds, such as:

```text
2026-08-12 03:32:54.148241
```

Fix:

```python
to_timestamp(col("created_at"))
to_timestamp(col("updated_at"))
```

### Orders

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

Completed tables:

```text
products
suppliers
stores
inventory
```

### Products

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

### Suppliers

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

### Stores

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

### Inventory

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

---

## 14. Inventory Business Validation

Standardized join:

```text
inventory
  -> products
  -> stores
```

Relationships:

```text
inventory.product_id → products.product_id
inventory.store_id   → stores.store_id
```

Business rule:

```text
quantity_on_hand <= reorder_level
    → REORDER
otherwise
    → OK
```

Initial batch result:

```text
OK        456
REORDER    44
Total      500
```

Percentages:

```text
OK       = 91.2%
REORDER  = 8.8%
```

---

## 15. Initial Workflow Automation

Initial workflow:

```text
retailpulse-standardization-workflow
```

Original design:

```text
On-demand start
      ↓
8 standardization jobs
      ↓
ALL 8 SUCCEEDED
      ↓
standardized crawler
```

Successful run:

```text
Started:   2026-08-18 23:38:38
Completed: 2026-08-18 23:41:41
Duration:  03 min 03 sec
```

Inventory validation after the run:

```text
record_count = 500
```

Audit timestamp:

```text
2026-08-18 23:40:46.882
```

This proved that the standardized data was actually reprocessed.

---

## 16. Positive Dynamic-`load_date` Test

Workflow property:

```text
load_date = 2026-08-14
```

All eight jobs reprocessed successfully.

Expected counts:

```text
customers      1000
orders         1987
order_items    3988
payments       1466
products         50
suppliers        10
stores           10
inventory       500
```

This validated workflow-driven partition selection across all eight jobs.

---

## 17. Negative Missing-Partition Test

The workflow was intentionally run with:

```text
load_date = 2026-08-19
```

No raw data existed for that date.

Because the empty-input checks were active, all eight standardization jobs failed.

Observed result:

```text
Workflow status:  COMPLETED
FailedActions:    8
SucceededActions: 0
```

Important lesson:

```text
Workflow status COMPLETED does not mean every action succeeded.
```

Because the jobs did not all succeed, the final ALL-success trigger was not satisfied and the standardized crawler did not run.

Historical standardized data remained preserved.

CLI troubleshooting lesson:

```powershell
aws glue get-workflow-runs `
  --name retailpulse-standardization-workflow `
  --region us-east-2 `
  --max-items 5
```

Use the exact returned `WorkflowRunId` before querying run properties.

---

## 18. Second Source Batch

### PostgreSQL insert run

```text
Customers inserted : 1000
Orders inserted    : 1975
Order items        : 3850
Payments inserted  : 1476
```

PostgreSQL totals:

```text
customers      2000
orders         3962
order_items    7838
payments       2942
```

### MySQL totals

```text
suppliers        20
products        100
stores            20
inventory       1000
```

Product range:

```text
1001 → 1100
```

---

## 19. PostgreSQL Generator Rerun Issue

The second PostgreSQL generator run initially failed with:

```text
psycopg.errors.UniqueViolation:
duplicate key value violates unique constraint "customers_email_key"
```

The generator used:

```python
fake.unique.email()
```

`fake.unique` only guarantees uniqueness during the current Faker execution and does not know what is already stored in PostgreSQL.

Fix:

```python
import uuid

f"{uuid.uuid4().hex}@example.com"
```

This kept the database `UNIQUE(email)` constraint intact while making repeated test-data generation safe.

Syntax validation:

```powershell
python -m py_compile .\src\data_generator\generate_postgres_data.py
```

---

## 20. MySQL Generator Rerun Issue

The MySQL generator originally generated fixed product IDs:

```text
1001–1050
```

A second run would create duplicate primary keys.

Fix:

```python
cursor.execute(
    "SELECT COALESCE(MAX(product_id), %s) FROM products",
    (PRODUCT_START_ID - 1,)
)

max_product_id = cursor.fetchone()[0]
start_product_id = max_product_id + 1
```

The second batch generated:

```text
1051–1100
```

Syntax validation:

```powershell
python -m py_compile .\src\data_generator\generate_mysql_data.py
```

---

## 21. Docker Port Issue During Second Batch

The containers temporarily exposed unexpected dynamically assigned host ports:

```text
MySQL      49680 → 3306
PostgreSQL 49681 → 5432
```

The project expected:

```text
MYSQL_PORT=3307
POSTGRES_PORT=5433
```

This caused local connection failures.

Fix: use explicit Compose mappings:

```yaml
ports:
  - "${MYSQL_PORT}:3306"
```

and:

```yaml
ports:
  - "${POSTGRES_PORT}:5432"
```

The stack was restarted without deleting the persistent volumes.

Safety rule:

```text
Do not use docker compose down -v
```

unless the database data is intentionally being deleted.

---

## 22. Direct Database-to-S3 Refactor

Old design:

```text
Database
   ↓
Python
   ↓
Local CSV
   ↓
boto3 upload_file()
   ↓
S3
```

New design:

```text
PostgreSQL / MySQL
        ↓
Python SELECT
        ↓
CSV created in memory
        ↓
boto3 put_object()
        ↓
S3 raw/
```

Implementation:

```python
import io

csv_buffer = io.StringIO()
```

Upload:

```python
s3.put_object(
    Bucket=S3_BUCKET,
    Key=s3_key,
    Body=csv_content.encode("utf-8"),
    ContentType="text/csv",
)
```

Updated scripts:

```text
src/ingestion/postgres_to_s3.py
src/ingestion/mysql_to_s3.py
```

The normal ingestion path no longer requires `data/extracts/`.

---

## 23. AWS Login Expiration During Ingestion

The first direct MySQL-to-S3 attempt successfully extracted data and created the CSV in memory, but S3 upload failed.

Error:

```text
AccessDeniedException:
The refresh token has expired.

LoginRefreshRequired:
Please reauthenticate using 'aws login'.
```

Fix:

```powershell
aws login
```

Verify:

```powershell
aws sts get-caller-identity
```

After reauthentication, the ingestion rerun completed successfully.

---

## 24. Second Raw S3 Batch — 2026-08-20

### MySQL

```text
suppliers  → 20 rows
products   → 100 rows
stores     → 20 rows
inventory  → 1000 rows
```

Paths:

```text
raw/mysql/suppliers/load_date=2026-08-20/
raw/mysql/products/load_date=2026-08-20/
raw/mysql/stores/load_date=2026-08-20/
raw/mysql/inventory/load_date=2026-08-20/
```

### PostgreSQL

```text
customers   → 2000 rows
orders      → 3962 rows
order_items → 7838 rows
payments    → 2942 rows
```

Paths:

```text
raw/postgres/customers/load_date=2026-08-20/
raw/postgres/orders/load_date=2026-08-20/
raw/postgres/order_items/load_date=2026-08-20/
raw/postgres/payments/load_date=2026-08-20/
```

All eight raw datasets were written directly from Python memory to S3.

---

## 25. Enhanced Glue Workflow with Raw Crawler

The workflow was rebuilt so the raw crawler executes first.

Final design:

```text
retailpulse-standardization-start
              ↓
retailpulse-raw-crawler
              ↓
raw crawler SUCCEEDED
              ↓
retailpulse-standardization-jobs-on-raw-success
              ↓
8 standardization jobs
              ↓
ALL 8 jobs SUCCEEDED
              ↓
retailpulse-standardized-crawler-after-jobs
              ↓
retailpulse-standardized-crawler
```

### Start trigger

```text
Name:   retailpulse-standardization-start
Type:   ON_DEMAND
Starts: retailpulse-raw-crawler
```

### Raw-crawler success trigger

```text
Name:
retailpulse-standardization-jobs-on-raw-success

Type:
CONDITIONAL / Event

Logic:
Start after ALL watched event

Watching:
retailpulse-raw-crawler - SUCCEEDED
```

Actions:

```text
8 standardization jobs
```

### Final success trigger

```text
Name:
retailpulse-standardized-crawler-after-jobs

Type:
CONDITIONAL

Logic:
Start after ALL watched event
```

Watching:

```text
retailpulse-standardize-customers - SUCCEEDED
retailpulse-standardize-orders - SUCCEEDED
retailpulse-standardize-order-items - SUCCEEDED
retailpulse_standardized.payments - SUCCEEDED
retailpulse-standardize-products - SUCCEEDED
retailpulse-standardize-suppliers - SUCCEEDED
retailpulse-standardize-stores - SUCCEEDED
retailpulse-standardize-inventory - SUCCEEDED
```

Starts:

```text
retailpulse-standardized-crawler
```

---

## 26. Dynamic Partition Overwrite

Old writer:

```python
(
    standardized_df
    .write
    .mode("overwrite")
    .partitionBy("load_date")
    .parquet(output_path)
)
```

Updated writer:

```python
(
    standardized_df
    .write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("load_date")
    .parquet(output_path)
)
```

Purpose:

```text
replace only the load_date partition being processed
preserve historical load_date partitions
support safe same-date reruns
```

Example:

```text
standardized/postgres/customers/
├── load_date=2026-08-14/   preserved
└── load_date=2026-08-20/   newly written
```

---

## 27. Multi-Batch Athena Validation

Final Athena validation:

| Table | 2026-08-14 | 2026-08-20 |
|---|---:|---:|
| customers | 1000 | 2000 |
| inventory | 500 | 1000 |
| order_items | 3988 | 7838 |
| orders | 1987 | 3962 |
| payments | 1466 | 2942 |
| products | 50 | 100 |
| stores | 10 | 20 |
| suppliers | 10 | 20 |

This proved:

```text
2026-08-14 history preserved
2026-08-20 partitions created
all eight tables queryable in Athena
raw crawler orchestration works
all eight standardization jobs succeed
standardized crawler runs after ALL jobs succeed
dynamic partition overwrite works
```

---

## 28. Current Ingestion Semantics

The pipeline now supports multiple `load_date` partitions, but source extraction is still a full snapshot.

Example:

```sql
SELECT *
FROM retail.customers;
```

Therefore:

```text
2026-08-14 customers = 1000 total rows
2026-08-20 customers = 2000 total rows
```

The August 20 partition contains the entire current source state, not only the newly inserted 1000 customers.

This was intentional for multi-batch validation.

---

## 29. Planned Event-Driven Start

The current workflow starts on demand.

Planned enhancement:

```text
raw batch upload completes
        ↓
write _SUCCESS marker
        ↓
Amazon EventBridge
        ↓
small Lambda
        ↓
StartWorkflowRun
with load_date
        ↓
Raw Crawler
        ↓
8 Standardization Jobs
        ↓
Standardized Crawler
```

Using one `_SUCCESS` marker is safer than starting a workflow for every S3 object because one source batch contains multiple files.

This is planned, not yet implemented.

---

## 30. Next Phase — True Incremental Ingestion

Current:

```sql
SELECT *
FROM source_table;
```

Target:

```text
last successful watermark
        ↓
read only new / changed records
        ↓
S3 incremental batch
        ↓
Glue
        ↓
later Snowflake MERGE / upsert
```

Conceptual SQL:

```sql
SELECT *
FROM source_table
WHERE updated_at > :last_successful_watermark;
```

Next-phase topics:

```text
watermark persistence
new/changed-row extraction
successful-run checkpointing
idempotent reruns
late-arriving data
MERGE/upsert patterns
record-count reconciliation
failure recovery
```

---

## 31. Key Lessons

1. Glue Crawlers register S3 schema and partition metadata.
2. Athena queries S3 through Glue Catalog metadata.
3. DynamicFrames are Glue-aware structures; Spark DataFrames are used for transformations.
4. `push_down_predicate` limits processing to the selected partition.
5. Workflow run properties remove hard-coded batch dates.
6. A missing f-string can cause a literal `{load_date}` partition predicate.
7. Jobs requiring `WORKFLOW_NAME` and `WORKFLOW_RUN_ID` should normally run through the workflow.
8. Empty-input checks make missing partitions fail clearly.
9. Glue workflow `COMPLETED` does not mean all actions succeeded.
10. Raw and standardized crawlers can be orchestrated in the same workflow.
11. ALL-success triggers prevent downstream catalog refresh after partial failure.
12. Standardized crawler paths should point to actual table roots.
13. Dynamic partition overwrite preserves historical partitions.
14. `standardized_at` is useful for batch auditing.
15. In-memory S3 upload removes unnecessary local file dependencies.
16. AWS CLI login credentials can expire and require `aws login`.
17. Synthetic generators must be rerunnable across batches.
18. The two-batch Athena test proves historical and current partitions can coexist.
19. The current solution is multi-batch but still full-snapshot.
20. Watermark-based extraction is the next architectural step.

---

## 32. Phase 3 Completion Checkpoint

```text
Raw S3 landing                              COMPLETE
Raw Glue Catalog                            COMPLETE
Athena raw validation                       COMPLETE
PostgreSQL standardization                  COMPLETE
MySQL standardization                       COMPLETE
Standardized Parquet                        COMPLETE
Standardized Catalog                        COMPLETE
Athena standardized joins                   COMPLETE
Inventory KPI validation                    COMPLETE
Glue Workflow                               COMPLETE
Dynamic workflow load_date                  COMPLETE
Empty-input protection                      COMPLETE
Positive workflow test                      COMPLETE
Negative missing-partition test             COMPLETE
Second source batch                         COMPLETE
Rerunnable PostgreSQL generator              COMPLETE
Rerunnable MySQL product generation          COMPLETE
Direct database-to-S3 ingestion             COMPLETE
Raw crawler workflow integration            COMPLETE
Dynamic partition overwrite                 COMPLETE
Historical partition preservation           COMPLETE
Second-batch standardized processing         COMPLETE
Two-batch Athena validation                  COMPLETE
EventBridge automatic workflow start         PLANNED
True watermark incremental ingestion         NEXT
```

**Phase 3 is successfully complete.**

The platform can now process a selected `load_date` through one Glue Workflow, automatically refresh the raw Catalog, execute all eight standardization jobs, preserve historical standardized partitions, refresh the standardized Catalog, and validate results through Athena.

**Next engineering phase:** implement true watermark-based incremental ingestion on a new feature branch.
