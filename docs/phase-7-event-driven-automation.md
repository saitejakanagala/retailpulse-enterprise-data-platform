# Phase 7 — Event-Driven Pipeline Automation

## Status

CORE EVENT-DRIVEN FLOW COMPLETE — RetailPulse now automatically hands completed RAW ingestion batches to AWS Glue through S3 events and Lambda, tracks batch lifecycle in DynamoDB, detects Glue success/failure through EventBridge, supports controlled retry of failed batches, and sends SNS email alerts for actual Glue processing failures.

`NO_DATA` marker handling remains a follow-up item before Phase 7 is considered fully closed.

---

## 1. Objective

Phase 6 completed the curated current-state Athena layer.

Phase 7 focuses on automating the operational flow between incremental source ingestion and the existing Glue standardization workflow.

Before Phase 7, the major processing pieces already existed:

```text
PostgreSQL / MySQL
        ↓
Watermark-based incremental ingestion
        ↓
Amazon S3 RAW
        ↓
AWS Glue standardization workflow
        ↓
Standardized Parquet
        ↓
Athena curated views
```

The missing part was a reliable batch-control layer that could answer:

> When RAW ingestion finishes, how does AWS know that a new batch is ready?

> How do we prevent a second batch from starting while the previous batch is unresolved?

> How do we track the lifecycle of each batch?

> How do we automatically react when Glue succeeds or fails?

> How do we retry a failed Glue batch without regenerating or re-ingesting the source data?

Phase 7 introduces that control and event-driven automation layer.

---

## 2. Phase 7 Architecture

The complete Phase 7 flow is:

```text
POSTGRESQL / MYSQL SOURCE SYSTEMS
        ↓
run_retailpulse_batch.py
        ↓
Check unresolved control markers
        ↓
Generate source changes
        ↓
Watermark-based incremental ingestion
        ↓
DynamoDB: retailpulse-watermarks
        ↓
Amazon S3 RAW
        ↓
DATA_AVAILABLE control marker
        ↓
S3 ObjectCreated event
        ↓
Lambda #1
retailpulse-start-glue-workflow
        ↓
DynamoDB: retailpulse-pipeline-runs
STARTING → RUNNING
        ↓
Glue Workflow
retailpulse-standardization-workflow
        ↓
retailpulse-raw-crawler
        ↓
8 standardization jobs
        ↓
retailpulse-standardized-crawler
        ↓
EventBridge
        ↓
Lambda #2
retailpulse-handle-glue-completion
        ↓
SUCCESS                         FAILURE
   ↓                               ↓
COMPLETING                     FAILED
   ↓                               ↓
Delete marker                  Keep marker
   ↓                               ↓
SUCCEEDED                      SNS email
                                   ↓
                            New batch blocked
                                   ↓
                            Lambda #3 retry
                                   ↓
                               RETRYING
                                   ↓
                                RUNNING
                                   ↓
                         Glue workflow rerun
```

---

## 3. New Phase 7 Orchestration Script

Phase 7 added:

```text
src/orchestration/run_retailpulse_batch.py
```

This script is the batch-level controller for the source-side ingestion process.

It does not contain the PostgreSQL or MySQL extraction logic itself.

Instead, it coordinates the existing source generators and ingestion scripts in the correct order.

The orchestration sequence is:

```text
Check unresolved markers
        ↓
Generate batch_id
        ↓
Generate load_date
        ↓
Run PostgreSQL source generator
        ↓
Run MySQL source generator
        ↓
Run PostgreSQL incremental ingestion
        ↓
Run MySQL incremental ingestion
        ↓
Find RAW objects created by this batch
        ↓
Create DATA_AVAILABLE marker
```

---

## 4. Unresolved Batch Protection

Before generating any new source data, the orchestration script checks:

```text
s3://retailpulse-data-lake-saiteja/
control/ingestion_batches/
```

The script looks for unresolved JSON control markers.

If one exists, the new batch is stopped before:

- generating source changes
- extracting source records
- uploading new RAW files
- advancing watermarks

This prevents overlapping batches.

The intended control rule is:

```text
Control marker exists
        ↓
Previous batch unresolved
        ↓
Do NOT start new ingestion
```

After a batch completes successfully, its control marker is deleted.

At that point:

```text
No unresolved marker
        ↓
Next batch may start
```

---

## 5. Batch ID

The orchestration script generates one batch identifier at the beginning of each execution.

Example:

```text
20260901T220645Z
```

Format:

```text
YYYYMMDDTHHMMSSZ
```

The same `batch_id` is used across the downstream pipeline so the ingestion batch can be traced through:

- S3 control marker
- DynamoDB pipeline-run record
- Glue workflow execution
- failure handling
- retry handling

The batch ID represents one logical RetailPulse ingestion cycle.

---

## 6. Load Date

The orchestration script also determines:

```text
load_date=YYYY-MM-DD
```

Example:

```text
load_date=2026-09-01
```

The same load date is used for RAW S3 partitions such as:

```text
raw/postgres/orders/load_date=2026-09-01/
raw/mysql/products/load_date=2026-09-01/
```

The load date is later passed to the Glue workflow so the standardization jobs process only that batch partition.

---

## 7. Source Data Generation

The orchestrator executes the existing data-generation scripts.

PostgreSQL generator:

```text
src/data_generator/generate_postgres_data.py
```

MySQL generator:

```text
src/data_generator/generate_mysql_data.py
```

The PostgreSQL generator creates/updates data for:

```text
customers
orders
order_items
payments
```

The MySQL generator creates/updates data for:

```text
suppliers
products
stores
inventory
```

The orchestrator invokes these scripts before incremental ingestion begins.

---

## 8. Incremental Ingestion Called by the Orchestrator

After source changes are generated, the orchestrator runs:

```text
src/ingestion/postgres_to_s3.py
src/ingestion/mysql_to_s3.py
```

These scripts continue to use the Phase 4 composite-watermark design.

DynamoDB table:

```text
retailpulse-watermarks
```

Primary key:

```text
source_table
```

Watermark:

```text
(updated_at, primary_key)
```

Conceptual incremental condition:

```sql
WHERE updated_at > :last_updated_at

OR

(
    updated_at = :last_updated_at
    AND primary_key > :last_primary_key
)
```

Watermarks are updated only after the corresponding S3 RAW upload succeeds.

Therefore:

```text
SOURCE DATABASE
        ↓
Read previous watermark
        ↓
Extract only new/updated rows
        ↓
Upload RAW CSV to S3
        ↓
Update watermark
```

---

## 9. MySQL Ingestion Correction

During Phase 7, the MySQL ingestion script was corrected so that the local working version performs actual MySQL incremental extraction.

The corrected implementation uses:

```text
mysql.connector
```

and applies the same composite-watermark pattern used by the PostgreSQL ingestion flow.

This correction is important because the previously committed Phase 4 version of `mysql_to_s3.py` contained incorrect PostgreSQL-oriented logic.

The corrected MySQL flow was tested successfully before the event-driven batch testing continued.

---

## 10. RAW Object Discovery

After both ingestion scripts complete, the orchestrator checks S3 for RAW objects created during the current batch.

It uses:

- the current `load_date`
- the batch start timestamp
- S3 object `LastModified`

This prevents old objects from the same date partition from being incorrectly counted as part of the current batch.

For a normal full batch, the expected source/table coverage is:

```text
PostgreSQL
    customers
    orders
    order_items
    payments

MySQL
    suppliers
    products
    stores
    inventory
```

Total expected RAW files:

```text
8
```

---

## 11. DATA_AVAILABLE Control Marker

After RAW ingestion is complete, the orchestrator creates a control marker.

Example:

```text
control/ingestion_batches/
load_date=2026-09-01/
20260901T220645Z_DATA_AVAILABLE.json
```

The marker contains metadata such as:

```json
{
  "batch_id": "20260901T220645Z",
  "load_date": "2026-09-01",
  "status": "DATA_AVAILABLE",
  "raw_file_count": 8,
  "raw_objects": [],
  "completed_at": "UTC timestamp"
}
```

The control marker is not business data.

Its purpose is to signal:

> RAW ingestion for this batch is complete and downstream processing may begin.

---

## 12. Why the Marker Is Created Last

The control marker is created only after:

```text
source generation
        ↓
PostgreSQL ingestion
        ↓
MySQL ingestion
        ↓
RAW S3 upload completion
        ↓
RAW object discovery
```

This prevents the Glue workflow from starting while RAW files are still being uploaded.

Therefore the marker behaves as the batch handoff signal between:

```text
SOURCE-SIDE INGESTION
```

and:

```text
AWS DOWNSTREAM PROCESSING
```

---

## 13. S3 Event Trigger

The S3 bucket is configured so that creation of a DATA_AVAILABLE marker automatically invokes Lambda #1.

Bucket:

```text
retailpulse-data-lake-saiteja
```

Event:

```text
s3:ObjectCreated:*
```

Prefix:

```text
control/ingestion_batches/
```

Suffix:

```text
_DATA_AVAILABLE.json
```

Therefore:

```text
DATA_AVAILABLE marker created
        ↓
S3 ObjectCreated event
        ↓
Lambda #1 automatically invoked
```

No manual Lambda test invocation is required for the normal pipeline flow.

---

## 14. Lambda #1 — Start Glue Workflow

Lambda function:

```text
retailpulse-start-glue-workflow
```

Lambda #1 is responsible for starting the downstream Glue processing for a valid ingestion batch.

Its flow is:

```text
Receive S3 event
        ↓
Read marker from S3
        ↓
Validate marker
        ↓
Create pipeline-run record
        ↓
STARTING
        ↓
Start Glue workflow
        ↓
Store glue_run_id
        ↓
RUNNING
```

---

## 15. Lambda #1 Marker Validation

Lambda #1 validates that:

- the object is under the expected control prefix
- the object name ends in `_DATA_AVAILABLE.json`
- the marker contains `batch_id`
- the marker contains `load_date`
- marker status is `DATA_AVAILABLE`

Only then does the function start the Glue workflow.

---

## 16. Pipeline Run DynamoDB Table

Phase 7 created:

```text
retailpulse-pipeline-runs
```

Primary key:

```text
batch_id
```

The table tracks downstream batch lifecycle.

This is different from:

```text
retailpulse-watermarks
```

The two tables have separate responsibilities.

### `retailpulse-watermarks`

Tracks:

> How far has source extraction progressed?

### `retailpulse-pipeline-runs`

Tracks:

> What is the current downstream processing state of this batch?

---

## 17. Pipeline Run Attributes

The `retailpulse-pipeline-runs` table is schemaless beyond the primary key.

Lambda functions dynamically add operational attributes such as:

```text
batch_id
load_date
status
marker_key
raw_file_count
created_at
started_at
glue_run_id
glue_completed_at
completed_at
failed_component
failed_component_type
failure_state
failed_at
retry_count
retry_requested_at
retried_at
previous_glue_run_id
```

---

## 18. Lambda #1 Idempotency

S3 event notifications can potentially be delivered more than once.

To prevent the same marker from starting Glue multiple times, Lambda #1 uses a conditional DynamoDB write.

Conceptually:

```text
attribute_not_exists(batch_id)
```

Behavior:

```text
First event
        ↓
No batch record exists
        ↓
Create STARTING item
        ↓
Start Glue
```

Duplicate event:

```text
Same batch_id already exists
        ↓
Do not start another workflow run
```

---

## 19. Glue Workflow Start

Lambda #1 starts:

```text
retailpulse-standardization-workflow
```

The workflow run receives runtime properties including:

```text
load_date
batch_id
marker_key
```

After Glue returns a workflow run ID, Lambda #1 stores it in:

```text
retailpulse-pipeline-runs
```

The batch status becomes:

```text
RUNNING
```

---

## 20. Glue Workflow `load_date` Run Property

The `load_date` workflow property tells the Glue jobs which RAW partition belongs to the current batch.

Example:

```text
load_date = 2026-09-16
```

The standardization jobs use logic such as:

```python
push_down_predicate = f"load_date='{load_date}'"
```

Therefore the jobs process only:

```text
load_date=2026-09-16
```

instead of scanning all historical partitions.

The workflow definition may display a default property value, but Lambda #1 can pass a run-specific `load_date` when starting each execution.

A complete workflow run was successfully validated again on:

```text
2026-09-16
```

with status:

```text
Completed
```

---

## 21. Glue Workflow Structure

The existing Glue workflow remains:

```text
retailpulse-standardization-workflow
```

Flow:

```text
retailpulse-raw-crawler
        ↓
8 standardization jobs
        ↓
retailpulse-standardized-crawler
```

---

## 22. RAW Crawler

Crawler:

```text
retailpulse-raw-crawler
```

The crawler scans the S3 RAW layer and updates the Glue Data Catalog.

It identifies:

- columns
- data types
- table metadata
- partitions such as `load_date`

The crawler does not physically move the RAW data into a relational database.

The actual CSV files remain in S3.

---

## 23. Standardization Jobs

The workflow contains these eight jobs:

```text
retailpulse-standardize-customers
retailpulse-standardize-orders
retailpulse-standardize-order-items
retailpulse_standardized.payments
retailpulse-standardize-suppliers
retailpulse-standardize-products
retailpulse-standardize-stores
retailpulse-standardize-inventory
```

Each job processes the workflow's current `load_date`.

The jobs transform:

```text
RAW CSV
        ↓
Standardization / typing / cleanup
        ↓
Parquet
        ↓
S3 standardized layer
```

---

## 24. Standardized Crawler

After the standardization jobs complete, the workflow runs:

```text
retailpulse-standardized-crawler
```

This crawler updates the Glue Data Catalog for the standardized Parquet data.

A successful completion of this final crawler represents successful completion of the downstream standardization flow.

---

## 25. EventBridge

Phase 7 added EventBridge rules to listen for Glue state changes.

EventBridge does not perform transformations.

Its role is:

```text
Glue emits state-change event
        ↓
EventBridge matches rule
        ↓
Lambda #2 invoked
```

Three rules were created.

---

## 26. EventBridge Rule — Standardized Crawler Success

Rule:

```text
retailpulse-standardized-crawler-success
```

Matches:

```text
source      = aws.glue
detail-type = Glue Crawler State Change
crawlerName = retailpulse-standardized-crawler
state       = Succeeded
```

Target:

```text
Lambda #2
retailpulse-handle-glue-completion
```

---

## 27. EventBridge Rule — Glue Job Failure

Rule:

```text
retailpulse-glue-job-failure
```

Matches Glue job states:

```text
FAILED
TIMEOUT
STOPPED
```

Target:

```text
Lambda #2
```

---

## 28. EventBridge Rule — Glue Crawler Failure

Rule:

```text
retailpulse-glue-crawler-failure
```

Matches crawler state:

```text
Failed
```

Target:

```text
Lambda #2
```

---

## 29. Lambda #2 — Completion Handler

Lambda function:

```text
retailpulse-handle-glue-completion
```

Lambda #2 handles both:

```text
successful downstream completion
```

and:

```text
Glue job/crawler failure
```

---

## 30. Lambda #2 Success Path

When:

```text
retailpulse-standardized-crawler
```

reports:

```text
Succeeded
```

Lambda #2 finds the current batch with:

```text
status = RUNNING
```

Then:

```text
RUNNING
        ↓
COMPLETING
        ↓
Delete DATA_AVAILABLE marker
        ↓
SUCCEEDED
```

Completion timestamps are written to:

```text
retailpulse-pipeline-runs
```

Deleting the marker releases the batch lock so the next ingestion batch may run.

---

## 31. Lambda #2 Failure Path

If one of the monitored Glue jobs reports:

```text
FAILED
TIMEOUT
STOPPED
```

or a crawler reports:

```text
Failed
```

Lambda #2 updates the current batch to:

```text
FAILED
```

It records:

```text
failed_component
failed_component_type
failure_state
failed_at
```

The control marker is intentionally kept.

Therefore:

```text
FAILED batch
        ↓
Marker still exists
        ↓
Next source batch is blocked
```

---

## 32. SNS Failure Notification

Phase 7 created the SNS topic:

```text
retailpulse-pipeline-alerts
```

An email subscription was confirmed.

Manual SNS delivery testing succeeded.

The final alerting design is:

```text
Actual Glue job/crawler failure
        ↓
EventBridge
        ↓
Lambda #2
        ↓
DynamoDB = FAILED
        ↓
SNS email
```

SNS notifications are used only by Lambda #2 for actual Glue processing failures.

No SNS notification was added to Lambda #1 or Lambda #3.

---

## 33. Lambda #3 — Failed Batch Retry

Phase 7 added:

```text
retailpulse-retry-failed-batch
```

Lambda #3 is intentionally manual.

There is no automatic retry trigger.

The reason is to avoid uncontrolled retry loops and allow the underlying issue to be investigated and fixed first.

---

## 34. Retryable States

Lambda #3 accepts batches in:

```text
FAILED
START_FAILED
RETRY_START_FAILED
```

A completed batch cannot be retried.

A test using an already:

```text
SUCCEEDED
```

batch correctly returned a rejection.

---

## 35. Retry Flow

For a failed batch, Lambda #3:

```text
Read pipeline-run record
        ↓
Validate retryable status
        ↓
Read original control marker
        ↓
Validate batch_id / load_date
        ↓
FAILED → RETRYING
        ↓
Increment retry_count
        ↓
Start same Glue workflow again
        ↓
Store new glue_run_id
        ↓
Store previous_glue_run_id
        ↓
RETRYING → RUNNING
```

The retry reuses:

```text
same batch_id
same load_date
same RAW data
same control marker
```

The source generators and ingestion scripts are not rerun.

---

## 36. First Fully Automatic Success Validation

Validated batch:

```text
batch_id  = 20260901T220645Z
load_date = 2026-09-01
```

Generated source changes included:

```text
PostgreSQL
customers     +100
orders        +445
order_items   +851
payments      +333

MySQL
suppliers     +1
stores        +0
products      +20
inventory     +100
```

Updates were also generated for existing rows.

Incremental ingestion produced:

```text
customers     114
orders        482
order_items   880
payments      355
suppliers     4
products      35
stores        2
inventory     300
```

Total RAW files:

```text
8
```

Control marker:

```text
control/ingestion_batches/load_date=2026-09-01/
20260901T220645Z_DATA_AVAILABLE.json
```

Glue workflow run:

```text
wr_a2257e6487b7c6f46ff3e2507e7e34a1a557c5f6c6f1d62ce179e3173c333eaf
```

Observed automatic flow:

```text
Orchestrator
        ↓
8 RAW files
        ↓
DATA_AVAILABLE marker
        ↓
S3 trigger
        ↓
Lambda #1
        ↓
STARTING → RUNNING
        ↓
Glue workflow
        ↓
Final crawler succeeds
        ↓
EventBridge
        ↓
Lambda #2
        ↓
SUCCEEDED
        ↓
Marker deleted
```

This validated the full automatic success path.

---

## 37. Controlled Failure Validation

A controlled failure was introduced in:

```text
retailpulse_standardized.payments
```

Failed batch:

```text
batch_id  = 20260903T194854Z
load_date = 2026-09-03
```

Initial Glue workflow run:

```text
wr_95485060647241b318bea7de1b39d58ac84ff7a0b36b72d7371501a540b358fb
```

DynamoDB recorded:

```text
status                = FAILED
failed_component      = retailpulse_standardized.payments
failed_component_type = GLUE_JOB
failure_state         = FAILED
raw_file_count        = 8
```

The marker remained in S3.

This confirmed:

```text
Glue failure
        ↓
EventBridge
        ↓
Lambda #2
        ↓
FAILED state
        ↓
Marker retained
```

---

## 38. Batch Lock Validation

After the controlled failure, the orchestration script was run again.

It correctly stopped with the unresolved-batch protection.

Observed message:

```text
Previous pipeline batch is still unresolved.
New source generation and RAW ingestion will NOT start.
```

This proved that a failed downstream batch prevents a new source batch from starting.

---

## 39. Retry Validation

After removing the controlled payments failure, Lambda #3 was invoked for:

```text
20260903T194854Z
```

New Glue workflow run:

```text
wr_9a09dd881f899d64f3b1c58a6d6607353a2601f70de2bb1eadbb2c21b359c853
```

Previous Glue workflow run:

```text
wr_95485060647241b318bea7de1b39d58ac84ff7a0b36b72d7371501a540b358fb
```

Observed retry flow:

```text
FAILED
        ↓
RETRYING
        ↓
new Glue workflow run
        ↓
RUNNING
        ↓
Glue succeeds
        ↓
EventBridge
        ↓
Lambda #2
        ↓
SUCCEEDED
        ↓
Marker deleted
```

This validated controlled recovery without re-ingesting source data.

---

## 40. Pipeline State Model

The successful state path is:

```text
STARTING
        ↓
RUNNING
        ↓
COMPLETING
        ↓
SUCCEEDED
```

Failure and recovery:

```text
STARTING
        ↓
RUNNING
        ↓
FAILED
        ↓
RETRYING
        ↓
RUNNING
        ↓
COMPLETING
        ↓
SUCCEEDED
```

---

## 41. Relationship to Phase 6 Curated Views

Phase 7 does not replace the Phase 6 Athena views.

The existing curated views remain:

```text
retailpulse_curated.order_items_current
retailpulse_curated.payments_current
```

Phase 7 ensures that new incremental source changes automatically reach the standardized historical layer.

Because the Phase 6 Athena views are logical views over standardized data, they automatically include the newly standardized records the next time they are queried.

No view refresh job is required.

---

## 42. SQS

Amazon SQS is not part of the current Phase 7 architecture.

The current event flow is direct:

```text
S3 event
        ↓
Lambda #1
```

and:

```text
Glue event
        ↓
EventBridge
        ↓
Lambda #2
```

No SQS queue exists between these components.

---

## 43. Phase 7 Source-Control Changes

Phase 7 development was performed on:

```text
feature/event-driven-automation
```

Known local Phase 7 source changes include:

```text
src/data_generator/generate_mysql_data.py
src/data_generator/generate_postgres_data.py
src/ingestion/mysql_to_s3.py
src/orchestration/run_retailpulse_batch.py
```

Phase 7 documentation should be stored as:

```text
docs/phase-7-event-driven-automation.md
```

The Lambda functions and AWS event/IAM configuration were created in AWS during this phase.

They should be source-controlled later as part of infrastructure/code hardening.

---

## 44. Current Development Execution Model

At the end of the currently completed Phase 7 implementation, the orchestration entry point is still executed from the local development environment.

The source code is maintained in Git/GitHub, but GitHub does not execute the pipeline.

Current entry point:

```text
Local development environment
        ↓
run_retailpulse_batch.py
        ↓
AWS
```

The downstream pipeline after the control marker is event-driven and automatic.

Moving the source-side orchestration to managed AWS compute is a later productionization enhancement and is not part of the already validated Phase 7 implementation.

---

## 45. Outstanding `NO_DATA` Case

The orchestrator currently supports the concept:

```text
raw_file_count = 0
        ↓
NO_DATA marker
```

However, the S3 Lambda #1 trigger currently listens only for:

```text
_DATA_AVAILABLE.json
```

Therefore a marker such as:

```text
_NO_DATA.json
```

would not currently be consumed.

Because the orchestration batch-lock check treats control JSON files as unresolved, an unhandled NO_DATA marker could block a future batch.

The planned fix is:

```text
NO_DATA marker
        ↓
Record NO_DATA state
        ↓
Do not start Glue
        ↓
Delete NO_DATA marker
        ↓
Allow next batch
```

This remains the main unfinished Phase 7 control-path item.

---

## 46. Current Phase 7 Limitations / Future Hardening

The following are not required for the already validated core flow but are known hardening items:

- complete `NO_DATA` handling
- move Lambda source code into the Git repository
- represent AWS configuration as Infrastructure as Code
- strengthen the orchestration lock if concurrent orchestrators are introduced
- replace Lambda #2 DynamoDB scan with a more scalable lookup/index if batch volume grows
- add stronger workflow-run correlation if concurrent batches are supported
- move the locally executed orchestration to managed AWS compute
- add unattended scheduling only after managed execution is stable

---

## 47. Phase 7 Validation Summary

### Orchestration

- Central orchestration script created — PASS
- Previous unresolved marker check — PASS
- Batch ID generation — PASS
- Load date generation — PASS
- PostgreSQL source generation — PASS
- MySQL source generation — PASS
- PostgreSQL incremental ingestion — PASS
- MySQL incremental ingestion — PASS
- RAW object discovery — PASS
- DATA_AVAILABLE marker creation — PASS

### Automatic workflow start

- S3 DATA_AVAILABLE trigger — PASS
- Lambda #1 invoked automatically — PASS
- Pipeline-run record created — PASS
- Duplicate-start protection added — PASS
- Glue workflow automatically started — PASS
- `glue_run_id` stored — PASS
- RUNNING status recorded — PASS

### Glue processing

- RAW crawler execution — PASS
- 8 standardization jobs — PASS
- Standardized crawler — PASS
- `load_date` workflow property processing — PASS
- Successful workflow run validated again on 2026-09-16 — PASS

### Completion handling

- EventBridge standardized-crawler success rule — PASS
- Lambda #2 success handling — PASS
- RUNNING → COMPLETING → SUCCEEDED — PASS
- Control marker deletion after success — PASS

### Failure handling

- EventBridge Glue-job failure rule — PASS
- EventBridge crawler-failure rule — PASS
- Controlled Glue failure detected — PASS
- FAILED state stored — PASS
- Failed component metadata stored — PASS
- Failed marker retained — PASS
- New source batch blocked — PASS

### Retry

- Lambda #3 created — PASS
- Manual failed-batch retry — PASS
- Retry guard for SUCCEEDED batch — PASS
- Same batch ID reused — PASS
- Same load date reused — PASS
- Same RAW data reused — PASS
- New Glue run ID created — PASS
- Previous Glue run ID retained — PASS
- Failed batch recovered to SUCCEEDED — PASS

### Alerting

- SNS topic created — PASS
- Email subscription confirmed — PASS
- Manual SNS delivery — PASS
- Lambda #2 failure notification logic added — PASS
- SNS intentionally not added to Lambda #1 — PASS
- SNS intentionally not added to Lambda #3 — PASS

### Remaining

- `NO_DATA` automatic cleanup — PENDING
- Phase 7 Git commit / push / PR — PENDING

---

## 48. Phase 7 Result

Phase 7 successfully converted the downstream RetailPulse processing path into a controlled event-driven pipeline.

The complete validated flow is now:

```text
PostgreSQL / MySQL
        ↓
Source change generation
        ↓
Watermark-based incremental ingestion
        ↓
DynamoDB watermarks
        ↓
S3 RAW
        ↓
DATA_AVAILABLE control marker
        ↓
S3 event
        ↓
Lambda #1
        ↓
DynamoDB pipeline-run tracking
        ↓
Glue standardization workflow
        ↓
RAW crawler
        ↓
8 standardization jobs
        ↓
Standardized Parquet
        ↓
Standardized crawler
        ↓
EventBridge
        ↓
Lambda #2
        ↓
SUCCESS or FAILURE
```

Successful batches:

```text
RUNNING
        ↓
COMPLETING
        ↓
Marker deleted
        ↓
SUCCEEDED
```

Failed batches:

```text
RUNNING
        ↓
FAILED
        ↓
Marker retained
        ↓
SNS alert
        ↓
New ingestion blocked
        ↓
Manual Lambda #3 retry
        ↓
RUNNING
        ↓
SUCCEEDED
```

The event-driven success path, failure path, unresolved-batch lock, manual retry, and recovery path have all been validated successfully.

The remaining Phase 7 work is to finalize `NO_DATA` handling and then commit/push the Phase 7 changes and documentation to GitHub.
