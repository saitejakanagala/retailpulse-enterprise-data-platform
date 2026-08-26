# Phase 5 — Incremental Standardization and Schema Evolution

## Status

COMPLETE — PostgreSQL `order_items` and `payments` incremental standardization enhancements were implemented, executed through the RetailPulse Glue workflow, and validated in Amazon Athena.

---

## 1. Objective

Phase 5 extends the existing AWS Glue standardization layer so that PostgreSQL records introduced through the Phase 4 watermark-based incremental ingestion process can be processed correctly.

Phase 4 introduced update tracking for PostgreSQL tables using:

- `updated_at`
- primary-key based composite watermarks
- incremental raw S3 batches
- DynamoDB watermark state

The PostgreSQL `order_items` and `payments` tables received new `updated_at` columns during Phase 4.

The Phase 5 goal was therefore to ensure that these new fields are correctly handled in the standardized Parquet layer.

---

## 2. Existing Pipeline

The relevant pipeline is:

```text
PostgreSQL
    ↓
Watermark-based incremental ingestion
    ↓
Amazon S3 raw CSV
    ↓
AWS Glue raw crawler
    ↓
Glue Data Catalog — retailpulse_raw
    ↓
Glue standardization jobs
    ↓
Amazon S3 standardized Parquet
    ↓
AWS Glue standardized crawler
    ↓
Amazon Athena
```

---

## 3. Raw Schema Evolution Verification

The existing raw crawler is:

`retailpulse-raw-crawler`

Target:

`s3://retailpulse-data-lake-saiteja/raw/`

Glue database:

`retailpulse_raw`

The crawler was rerun after the Phase 4 PostgreSQL schema migration.

Before rerunning the crawler:

### order_items

- `order_item_id`
- `order_id`
- `product_id`
- `quantity`
- `unit_price`
- `created_at`

### payments

- `payment_id`
- `order_id`
- `payment_method`
- `payment_status`
- `amount`
- `transaction_reference`
- `payment_timestamp`

The new `updated_at` column was not yet present in the Glue Data Catalog.

After rerunning the crawler, Glue detected:

### order_items

`updated_at string`

### payments

`updated_at string`

The raw layer keeps the timestamp value as a string because the source files are CSV.

---

## 4. Glue Workflow

Existing workflow:

`retailpulse-standardization-workflow`

The workflow orchestrates:

1. Raw Glue crawler
2. Standardization jobs
3. Standardized Glue crawler

The existing workflow was reused rather than creating another workflow.

For Phase 5 validation, the workflow property was configured with:

`load_date = 2026-08-22`

The workflow completed successfully.

---

## 5. Why Only order_items and payments Were Changed

Phase 5 code changes were made only to the two PostgreSQL Glue standardization jobs that required handling of the new Phase 4 schema evolution:

- `order_items`
- `payments`

The Phase 4 PostgreSQL migration added `updated_at` to these two tables.

The remaining Glue standardization jobs were not modified during this Phase 5 implementation.

---

## 6. Source-Controlled Glue Scripts

The tested Glue scripts were copied from their S3 runtime locations into the Git repository.

Repository paths:

`src/glue/standardization/retailpulse-standardize-order-items.py`

`src/glue/standardization/retailpulse_standardized.payments.py`

Runtime S3 locations:

`s3://retailpulse-data-lake-saiteja/glue/scripts/retailpulse-standardize-order-items.py`

`s3://retailpulse-data-lake-saiteja/glue/scripts/retailpulse_standardized.payments.py`

S3 remains the runtime script location used by AWS Glue.

Git/GitHub is used for source-code version control, review, and project history.

These Glue scripts were not previously tracked in the repository. Phase 5 adds the tested versions to source control for the first time.

---

## 7. order_items Standardization Changes

The `order_items` Glue job was enhanced to support the Phase 4 `updated_at` field and incremental update-aware processing.

### 7.1 Existing Workflow-Based load_date

The job continues to obtain `load_date` from Glue workflow run properties.

The job reads only the raw partition matching the current workflow `load_date`:

```python
order_items_dynamic_frame = (
    glueContext.create_dynamic_frame.from_catalog(
        database="retailpulse_raw",
        table_name="order_items",
        push_down_predicate=f"load_date='{load_date}'"
    )
)
```

This prevents the job from processing all historical raw partitions on every run.

### 7.2 Zero-Row Processing

Previously, an empty raw partition caused the Glue job to fail.

For incremental ingestion, zero new rows is a valid condition.

The updated job now:

- logs that no incremental data exists
- commits the Glue job successfully
- exits with status 0

Example logic:

```python
if order_items_df.rdd.isEmpty():
    print(
        f"No incremental order_items data found "
        f"for load_date={load_date}. Nothing to process."
    )

    job.commit()
    sys.exit(0)
```

### 7.3 Existing Data-Quality Rules Preserved

The following validations remain in place:

- `order_item_id` must not be NULL
- `order_id` must not be NULL
- `product_id` must not be NULL
- `quantity` must be greater than zero
- `unit_price` must be NULL or greater than/equal to zero

### 7.4 Timestamp Conversion

The raw crawler registers the CSV field as:

`updated_at string`

The Glue transformation converts it to a timestamp:

```python
.withColumn(
    "updated_at",
    to_timestamp(col("updated_at"))
)
```

The existing `created_at` conversion is also preserved:

```python
.withColumn(
    "created_at",
    to_timestamp(col("created_at"))
)
```

The standardized Parquet schema therefore stores:

- `created_at timestamp`
- `updated_at timestamp`

### 7.5 Latest Record Selection

The Phase 3 implementation used:

```python
.dropDuplicates(["order_item_id"])
```

Spark `dropDuplicates()` does not guarantee that the newest version of a duplicate business key will be retained.

Phase 5 replaces that logic with a deterministic Spark window.

```python
latest_record_window = (
    Window
    .partitionBy("order_item_id")
    .orderBy(
        col("updated_at").desc(),
        col("created_at").desc()
    )
)
```

A row number is assigned:

```python
.withColumn(
    "row_num",
    row_number().over(latest_record_window)
)
```

Only:

`row_num = 1`

is retained.

The temporary `row_num` column is then removed.

This keeps the newest version of each `order_item_id` within the currently processed `load_date` partition.

### 7.6 standardized_at

The job continues to add:

`standardized_at`

using:

```python
.withColumn(
    "standardized_at",
    current_timestamp()
)
```

This is different from `updated_at`.

- `updated_at` = when the source PostgreSQL record was updated
- `standardized_at` = when AWS Glue processed the record

### 7.7 Standardized Write

Output path:

`s3://retailpulse-data-lake-saiteja/standardized/postgres/order_items/`

The job continues to write Parquet using:

```python
standardized_order_items_df \
    .write \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .partitionBy("load_date") \
    .parquet(output_path)
```

This dynamically overwrites only the `load_date` partition being processed.

---

## 8. payments Standardization Changes

The `payments` Glue job received the same incremental-processing enhancement.

### 8.1 Existing Workflow-Based load_date

The job continues to read only the raw `payments` partition matching the workflow `load_date`.

```python
payments_dynamic_frame = (
    glueContext.create_dynamic_frame.from_catalog(
        database="retailpulse_raw",
        table_name="payments",
        push_down_predicate=f"load_date='{load_date}'"
    )
)
```

### 8.2 Zero-Row Processing

An empty incremental payments partition is now treated as a successful no-op instead of a Glue job failure.

### 8.3 Existing Payment Transformations Preserved

The following transformations remain:

- required `payment_id`
- required `order_id`
- non-negative payment amount
- uppercase `payment_method`
- uppercase `payment_status`
- trimmed `transaction_reference`
- amount cast to `decimal(12,2)`
- `payment_timestamp` converted to timestamp
- `standardized_at` added

### 8.4 updated_at Conversion

The new raw field:

`updated_at string`

is converted using:

```python
.withColumn(
    "updated_at",
    to_timestamp(col("updated_at"))
)
```

The standardized schema therefore stores:

`updated_at timestamp`

### 8.5 Latest Record Selection

The previous implementation used:

```python
.dropDuplicates(["payment_id"])
```

Phase 5 replaces that with:

```python
latest_payment_window = (
    Window
    .partitionBy("payment_id")
    .orderBy(
        col("updated_at").desc(),
        col("payment_timestamp").desc()
    )
)
```

A row number is assigned and only the newest record is kept.

This selects the latest version of a `payment_id` within the currently processed `load_date` partition.

### 8.6 Standardized Write

Output path:

`s3://retailpulse-data-lake-saiteja/standardized/postgres/payments/`

The job continues to use dynamic partition overwrite with `load_date`.

---

## 9. Partition Strategy and Historical Preservation

Both jobs continue to use:

`load_date`

as the standardized S3 partition key.

Write behavior:

```python
.mode("overwrite")
.option("partitionOverwriteMode", "dynamic")
.partitionBy("load_date")
```

This means:

- rerunning the same `load_date` replaces that standardized partition
- earlier `load_date` partitions remain intact
- updated versions of existing business keys can appear again in later partitions
- history is preserved across different load dates

Example:

```text
load_date=2026-08-20
    order_item_id=1

load_date=2026-08-22
    order_item_id=1
```

Both versions can exist in the standardized dataset.

The Phase 5 `row_number()` deduplication operates only inside the currently processed `load_date`.

It does not delete historical versions from earlier partitions.

---

## 10. Workflow Validation

The workflow used was:

`retailpulse-standardization-workflow`

Validation property:

`load_date = 2026-08-22`

The workflow completed successfully.

The modified jobs:

- `retailpulse-standardize-order-items`
- `retailpulse_standardized.payments`

both completed successfully.

The standardized crawler also ran as part of the workflow and updated the standardized Glue Data Catalog.

---

## 11. Athena Validation — order_items

Database:

`retailpulse_standardized`

Athena `DESCRIBE order_items` confirmed:

- `order_item_id` — bigint
- `order_id` — bigint
- `product_id` — bigint
- `quantity` — bigint
- `unit_price` — double
- `created_at` — timestamp
- `standardized_at` — timestamp
- `updated_at` — timestamp
- `load_date` — string partition

### 11.1 Row Count

For:

`load_date = 2026-08-22`

Athena returned:

`4,142 rows`

### 11.2 updated_at Range

Earliest `updated_at`:

`2026-08-22 18:00:56.272`

Latest `updated_at`:

`2026-08-22 18:17:28.894`

Total rows:

`4,142`

### 11.3 Duplicate Validation

The following query was used:

```sql
SELECT
    order_item_id,
    COUNT(*) AS record_count
FROM retailpulse_standardized.order_items
WHERE load_date = '2026-08-22'
GROUP BY order_item_id
HAVING COUNT(*) > 1;
```

Result:

`No results`

Therefore:

`0 duplicate order_item_id values`

exist inside the `2026-08-22` standardized partition.

---

## 12. Historical Record Validation — order_item_id = 1

`order_item_id = 1` was queried across standardized partitions.

Athena showed versions from multiple load dates:

- `2026-08-14`
- `2026-08-20`
- `2026-08-22`

The `2026-08-22` version contained:

`updated_at = 2026-08-22 18:00:56.272`

Older standardized partitions contained NULL for `updated_at`.

This is expected because those Parquet files were created before Phase 4 introduced the `updated_at` column to `order_items`.

The older partitions were not rewritten.

This confirms:

- historical partitions were preserved
- the updated record version was captured in the later partition
- the standardized layer supports historical version retention across `load_date`

---

## 13. Athena Validation — payments

Athena `DESCRIBE payments` confirmed:

- `payment_id` — bigint
- `order_id` — bigint
- `payment_method` — string
- `payment_status` — string
- `amount` — decimal(12,2)
- `transaction_reference` — string
- `payment_timestamp` — timestamp
- `standardized_at` — timestamp
- `updated_at` — timestamp
- `load_date` — string partition

### 13.1 Row Count

For:

`load_date = 2026-08-22`

Athena returned:

`1,517 rows`

### 13.2 Duplicate Validation

The following query was used:

```sql
SELECT
    payment_id,
    COUNT(*) AS record_count
FROM retailpulse_standardized.payments
WHERE load_date = '2026-08-22'
GROUP BY payment_id
HAVING COUNT(*) > 1;
```

Result:

`No results`

Therefore:

`0 duplicate payment_id values`

exist inside the `2026-08-22` standardized partition.

---

## 14. Phase 5 Validation Summary

### order_items

- Raw schema evolution detected by Glue crawler — PASS
- `updated_at` detected in raw Glue Data Catalog — PASS
- `updated_at` converted from string to timestamp — PASS
- 4,142 records standardized for `2026-08-22` — PASS
- Duplicate `order_item_id` validation — PASS
- Historical partitions preserved — PASS
- Updated historical record detected — PASS

### payments

- Raw schema evolution detected by Glue crawler — PASS
- `updated_at` detected in raw Glue Data Catalog — PASS
- `updated_at` converted from string to timestamp — PASS
- 1,517 records standardized for `2026-08-22` — PASS
- Duplicate `payment_id` validation — PASS

---

## 15. Source Control Strategy

The AWS Glue jobs continue to execute scripts from S3.

Runtime scripts:

```text
s3://retailpulse-data-lake-saiteja/glue/scripts/
```

The final tested scripts were copied into the local repository under:

```text
src/glue/standardization/
```

Files:

```text
retailpulse-standardize-order-items.py
retailpulse_standardized.payments.py
```

Purpose:

- AWS S3 = Glue runtime script location
- Git/GitHub = version-controlled source code and history

This allows Phase 5 changes to be reviewed and merged through a Pull Request without rewriting historical Phase 3 commits.

---

## 16. Relationship to Phase 3 and Phase 4

### Phase 3

Phase 3 established:

- raw Glue crawler
- Glue standardization jobs
- standardized Parquet layer
- Glue workflow orchestration
- standardized crawler
- Athena validation

Phase 3 remains preserved in Git history and documentation.

### Phase 4

Phase 4 introduced:

- watermark-based incremental ingestion
- DynamoDB watermark state
- `updated_at + primary key` composite checkpoints
- PostgreSQL update tracking
- new `updated_at` columns for `order_items` and `payments`
- incremental S3 raw batches

### Phase 5

Phase 5 connects those Phase 4 changes to the downstream standardization layer by:

- updating the raw Glue catalog
- converting `updated_at` to timestamp
- selecting latest records within each incremental batch
- supporting zero-row incremental runs
- preserving historical standardized partitions
- validating results in Athena
- adding the tested Glue code to Git source control

---

## 17. Important Architectural Behavior

The standardized layer currently behaves as a historical partitioned dataset.

It is not a single-row-per-business-key current-state table.

For current-state reporting, Athena can later derive the newest record with logic such as:

```sql
ROW_NUMBER() OVER (
    PARTITION BY order_item_id
    ORDER BY updated_at DESC
)
```

and keep only:

`row_number = 1`

A future architecture enhancement could use Apache Iceberg with MERGE/upsert semantics to maintain dedicated current-state tables while retaining historical datasets separately.

---

## 18. Phase 5 Result

Phase 5 successfully extended the RetailPulse standardization layer to support PostgreSQL schema evolution and update-aware incremental processing.

The validated flow is now:

```text
PostgreSQL INSERT / UPDATE
        ↓
updated_at tracking
        ↓
DynamoDB composite watermark
        ↓
Incremental raw S3 batch
        ↓
Glue raw crawler
        ↓
Glue schema evolution
        ↓
Incremental Glue standardization
        ↓
Historical standardized Parquet partitions
        ↓
Standardized Glue crawler
        ↓
Amazon Athena validation
```

Phase 5 validation completed successfully.
