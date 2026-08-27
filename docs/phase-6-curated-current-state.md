# Phase 6 — Curated Current-State Views

## Status

COMPLETE — Amazon Athena current-state views were created for PostgreSQL `order_items` and `payments`, validated against the historical standardized layer, and their SQL definitions were added to source control.

---

## 1. Objective

The RetailPulse standardized layer preserves historical versions of records across `load_date` partitions.

For example, the same business key can appear in multiple standardized partitions:

```text
load_date=2026-08-14
    order_item_id=1

load_date=2026-08-20
    order_item_id=1

load_date=2026-08-22
    order_item_id=1
```

This historical design is useful for auditing, change analysis, replay, debugging, and understanding how records evolved over time.

However, analytics users also need a simple way to answer:

> What is the latest/current version of each business record?

Phase 6 introduces a curated consumption layer using Amazon Athena views.

The curated views return one latest record per primary key while leaving the historical standardized dataset unchanged.

---

## 2. Architecture

The RetailPulse platform now has three logical data layers:

```text
SOURCE SYSTEMS
PostgreSQL / MySQL
        ↓
Watermark-based incremental ingestion
        ↓
Amazon S3 RAW
        ↓
AWS Glue standardization
        ↓
STANDARDIZED HISTORICAL LAYER
        ↓
Amazon Athena current-state views
        ↓
CURATED ANALYTICS LAYER
```

For the PostgreSQL tables implemented in Phase 6:

```text
retailpulse_standardized.order_items
        ↓
retailpulse_curated.order_items_current

retailpulse_standardized.payments
        ↓
retailpulse_curated.payments_current
```

---

## 3. Relationship to the Existing Glue Workflow

The curated Athena views are **not executed by the Glue workflow**.

The existing Glue workflow continues to handle:

```text
Raw S3
    ↓
Raw crawler
    ↓
Glue standardization jobs
    ↓
Standardized Parquet
    ↓
Standardized crawler
```

The Athena curated views sit **above** the standardized layer.

They do not require a new Glue job, a new Glue crawler, a scheduled refresh, a separate S3 write, or a new Parquet copy.

When analysts query a view, Athena evaluates the saved SQL against the current standardized table contents.

Therefore, once new standardized data arrives, the view automatically reflects the new current state the next time it is queried.

---

## 4. Why a Curated View Is Needed

The standardized tables preserve historical record versions.

Example:

```text
order_item_id   updated_at              load_date
-------------   ----------------------  ----------
1               NULL                    2026-08-14
1               NULL                    2026-08-20
1               2026-08-22 18:00:56     2026-08-22
```

All three records are useful historically.

However, an analytics user asking for the current state of `order_item_id = 1` should receive only the newest version.

The curated view performs this latest-record selection automatically.

---

## 5. Athena Curated Database

The curated database was created in Athena:

```sql
CREATE DATABASE IF NOT EXISTS retailpulse_curated;
```

The views created in Phase 6 are:

```text
retailpulse_curated.order_items_current
retailpulse_curated.payments_current
```

No physical copy of the historical data is created by these views.

---

## 6. What an Athena View Is

An Athena view is a saved SQL query.

For example:

```sql
SELECT *
FROM retailpulse_curated.order_items_current;
```

does not read a separate physical `order_items_current.parquet` dataset.

Instead, Athena evaluates the stored SQL logic against:

```text
retailpulse_standardized.order_items
```

The view therefore acts as a logical analytics layer on top of the historical standardized dataset.

---

## 7. Two Different Meanings of Partition

The Phase 6 design uses the word `partition` in two different contexts.

### 7.1 Physical S3 / Glue Partition

Example:

```text
load_date=2026-08-22/
```

This physically organizes data in S3.

Purpose:

- organize data by pipeline load date
- preserve historical batches
- support partition pruning
- reduce Athena scan cost
- improve query performance

### 7.2 SQL Window `PARTITION BY`

Example:

```sql
PARTITION BY order_item_id
```

This does **not** create S3 folders.

It temporarily groups rows with the same business key so SQL window functions such as `ROW_NUMBER()` can rank versions within that group.

These are completely different concepts.

---

## 8. order_items Current-State View

Source table:

```text
retailpulse_standardized.order_items
```

Curated view:

```text
retailpulse_curated.order_items_current
```

Source-controlled SQL:

```text
sql/athena/curated/order_items_current.sql
```

### 8.1 View Definition

```sql
CREATE OR REPLACE VIEW retailpulse_curated.order_items_current AS

WITH ranked_order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        created_at,
        updated_at,
        standardized_at,
        load_date,

        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY
                COALESCE(updated_at, created_at) DESC,
                load_date DESC,
                standardized_at DESC
        ) AS rn

    FROM retailpulse_standardized.order_items
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    created_at,
    updated_at,
    standardized_at,
    load_date
FROM ranked_order_items
WHERE rn = 1;
```

---

## 9. order_items View Logic Explained

### 9.1 Read Historical Data

The view reads `retailpulse_standardized.order_items`, which can contain multiple versions of the same `order_item_id`.

### 9.2 Group by Business Key

```sql
PARTITION BY order_item_id
```

groups all versions of the same order item together for ranking.

### 9.3 Sort Newest to Oldest

```sql
ORDER BY
    COALESCE(updated_at, created_at) DESC,
    load_date DESC,
    standardized_at DESC
```

Priority:

1. newest source update timestamp
2. newest pipeline load date
3. newest Glue processing timestamp

### 9.4 Why `COALESCE(updated_at, created_at)` Is Used

Some older historical `order_items` records have `updated_at = NULL` because those Parquet files were created before Phase 4 added the `updated_at` column.

`COALESCE()` means use `updated_at` when it exists; otherwise use `created_at`.

### 9.5 Assign Row Numbers

`ROW_NUMBER()` assigns:

```text
rn = 1 → newest version
rn = 2 → second newest
rn = 3 → older version
```

### 9.6 Keep Only the Latest Version

```sql
WHERE rn = 1
```

returns exactly one latest row for each `order_item_id`.

Historical records remain untouched in the standardized table.

---

## 10. order_items Validation

The known test record `order_item_id = 1` was queried from the curated view.

The view returned only:

```text
order_item_id    = 1
quantity         = 2
created_at       = 2026-08-12 03:32:54.148
updated_at       = 2026-08-22 18:00:56.272
standardized_at  = 2026-08-26 22:27:37.608
load_date        = 2026-08-22
```

The standardized historical table contains three versions of `order_item_id = 1`:

```text
2026-08-14
2026-08-20
2026-08-22
```

The curated view correctly selected only the latest version.

---

## 11. order_items Historical vs Current Counts

Athena validation returned:

```text
Historical rows            = 15,968
Current rows               = 11,979
Distinct order_item IDs    = 11,979
```

Because `current_rows = distinct_order_item_ids`, the curated view contains exactly one current row per `order_item_id`.

Extra historical rows:

```text
15,968 - 11,979 = 3,989
```

Athena also returned:

```text
order_item_ids_with_multiple_versions = 3,988
```

One order item has three historical versions:

```text
order_item_id = 1
version_count = 3
```

This validates that the standardized table preserves history while the curated view presents current state.

---

## 12. payments Current-State View

Source table:

```text
retailpulse_standardized.payments
```

Curated view:

```text
retailpulse_curated.payments_current
```

Source-controlled SQL:

```text
sql/athena/curated/payments_current.sql
```

### 12.1 View Definition

```sql
CREATE OR REPLACE VIEW retailpulse_curated.payments_current AS

WITH ranked_payments AS (
    SELECT
        payment_id,
        order_id,
        payment_method,
        payment_status,
        amount,
        transaction_reference,
        payment_timestamp,
        updated_at,
        standardized_at,
        load_date,

        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY
                COALESCE(updated_at, payment_timestamp) DESC,
                load_date DESC,
                standardized_at DESC
        ) AS rn

    FROM retailpulse_standardized.payments
)

SELECT
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    transaction_reference,
    payment_timestamp,
    updated_at,
    standardized_at,
    load_date
FROM ranked_payments
WHERE rn = 1;
```

---

## 13. payments View Logic

The payments view uses the same latest-record pattern.

Rows are grouped by `payment_id` and sorted by:

```sql
COALESCE(updated_at, payment_timestamp) DESC,
load_date DESC,
standardized_at DESC
```

Older historical payment partitions may have `updated_at = NULL`, so `payment_timestamp` is used as a fallback.

Only `rn = 1` is returned, producing one current row per `payment_id`.

---

## 14. payments Validation

Athena returned:

```text
current_rows            = 4,459
distinct_payment_ids    = 4,459
```

Because the values match, the view contains exactly one current row per payment ID.

Historical comparison returned:

```text
Historical rows = 5,925
Current rows    = 4,459
```

Extra historical rows:

```text
5,925 - 4,459 = 1,466
```

Athena also returned:

```text
payment_ids_with_multiple_versions = 1,466
```

This confirms that the standardized historical payments table contains repeated versions while the curated view keeps only the latest record for each payment ID.

---

## 15. Historical Layer vs Curated Layer

The two layers answer different questions.

### Standardized Historical Layer

```sql
SELECT *
FROM retailpulse_standardized.order_items
WHERE order_item_id = 1
ORDER BY load_date;
```

Purpose:

> Show every historical version available for this business key.

### Curated Current-State View

```sql
SELECT *
FROM retailpulse_curated.order_items_current
WHERE order_item_id = 1;
```

Purpose:

> Show only the latest/current version.

---

## 16. Analytics Usage

### 16.1 Current State of All Order Items

```sql
SELECT *
FROM retailpulse_curated.order_items_current;
```

### 16.2 Current State of All Payments

```sql
SELECT *
FROM retailpulse_curated.payments_current;
```

### 16.3 Records That Arrived in a Particular Pipeline Batch

If analysts want to know what arrived in a specific pipeline load, they should query the standardized historical table:

```sql
SELECT *
FROM retailpulse_standardized.order_items
WHERE load_date = '2026-08-22';
```

### 16.4 Current Records Whose Latest Version Came from a Specific Load Date

```sql
SELECT *
FROM retailpulse_curated.order_items_current
WHERE load_date = '2026-08-22';
```

These are different analytical questions.

---

## 17. Automatic View Behavior

The Athena views do not need to be refreshed manually.

Suppose a future update arrives:

```text
order_item_id = 1
updated_at    = 2026-08-27
load_date     = 2026-08-27
```

After the existing ingestion and Glue standardization pipeline writes the new standardized partition, the next query against `retailpulse_curated.order_items_current` automatically considers the new version.

The ranking becomes conceptually:

```text
2026-08-27 → rn = 1
2026-08-22 → rn = 2
2026-08-20 → rn = 3
2026-08-14 → rn = 4
```

The view definition itself does not need to change.

---

## 18. Source Control

Phase 6 stores the Athena view definitions in Git.

Repository structure:

```text
sql/
└── athena/
    └── curated/
        ├── order_items_current.sql
        └── payments_current.sql
```

Phase 6 documentation:

```text
docs/phase-6-curated-current-state.md
```

This makes the curated analytics logic reproducible instead of leaving it only inside the Athena console.

---

## 19. Why Iceberg Was Not Required Here

The standardized Parquet layer already preserves history.

For the current project scale, Athena views provide a simple way to derive current state without introducing another physical table format.

Apache Iceberg may be added later if the platform needs:

- physical current-state tables
- `MERGE`
- `UPDATE`
- `DELETE`
- ACID transactions
- snapshot-based time travel
- more advanced table metadata management

Phase 6 therefore uses views first to keep the architecture simple and intentional.

---

## 20. Phase 6 Validation Summary

### order_items_current

- Curated view created — PASS
- One row per `order_item_id` — PASS
- Current rows = 11,979 — PASS
- Distinct IDs = 11,979 — PASS
- Historical rows = 15,968 — PASS
- IDs with multiple historical versions = 3,988 — PASS
- `order_item_id = 1` historical version count = 3 — PASS
- Latest version correctly selected — PASS

### payments_current

- Curated view created — PASS
- One row per `payment_id` — PASS
- Current rows = 4,459 — PASS
- Distinct IDs = 4,459 — PASS
- Historical rows = 5,925 — PASS
- IDs with multiple historical versions = 1,466 — PASS

---

## 21. Phase 6 Result

Phase 6 successfully added a curated analytics layer without modifying or deleting historical standardized data.

The complete logical flow is now:

```text
PostgreSQL / MySQL
        ↓
Watermark-based incremental ingestion
        ↓
Amazon S3 RAW
        ↓
AWS Glue raw crawler
        ↓
AWS Glue standardization
        ↓
Historical standardized Parquet
        ↓
Standardized Glue Data Catalog
        ↓
Amazon Athena
        ↓
Curated current-state views
        ↓
Analytics consumers
```

The standardized layer answers:

> What versions have been loaded historically?

The curated layer answers:

> What is the latest/current version of each business record?

Phase 6 validation completed successfully.
