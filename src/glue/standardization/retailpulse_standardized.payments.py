import sys
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    current_timestamp,
    row_number
)

from pyspark.sql.window import Window


# ---------------------------------------------------------
# 1. Get Glue job + workflow information
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'WORKFLOW_NAME',
        'WORKFLOW_RUN_ID'
    ]
)


# ---------------------------------------------------------
# 2. Read load_date from the Glue Workflow
# ---------------------------------------------------------

glue_client = boto3.client("glue")

workflow_properties = glue_client.get_workflow_run_properties(
    Name=args['WORKFLOW_NAME'],
    RunId=args['WORKFLOW_RUN_ID']
)["RunProperties"]

if "load_date" not in workflow_properties:
    raise Exception("Workflow property 'load_date' is missing")

load_date = workflow_properties["load_date"]

print(f"Processing payments for load_date: {load_date}")


# ---------------------------------------------------------
# 3. Initialize Spark and Glue
# ---------------------------------------------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args['JOB_NAME'],
    args
)


# ---------------------------------------------------------
# 4. Read payments from Glue Data Catalog
#    Process only the workflow load_date
# ---------------------------------------------------------

payments_dynamic_frame = (
    glueContext.create_dynamic_frame.from_catalog(
        database="retailpulse_raw",
        table_name="payments",
        push_down_predicate=f"load_date='{load_date}'"
    )
)

payments_df = payments_dynamic_frame.toDF()


# ---------------------------------------------------------
# 5. Stop successfully if no incremental data exists
# ---------------------------------------------------------

if payments_df.rdd.isEmpty():
    print(
        f"No incremental payments data found "
        f"for load_date={load_date}. Nothing to process."
    )

    job.commit()
    sys.exit(0)


# ---------------------------------------------------------
# 6. Standardize payments data
# ---------------------------------------------------------

standardized_payments_df = (
    payments_df

    # Required identifiers
    .filter(
        col("payment_id").isNotNull()
    )

    .filter(
        col("order_id").isNotNull()
    )

    # Payment amount cannot be negative
    .filter(
        col("amount").isNull() |
        (col("amount") >= 0)
    )

    # Standardize payment method
    .withColumn(
        "payment_method",
        upper(trim(col("payment_method")))
    )

    # Standardize payment status
    .withColumn(
        "payment_status",
        upper(trim(col("payment_status")))
    )

    # Clean transaction reference
    .withColumn(
        "transaction_reference",
        trim(col("transaction_reference"))
    )

    # Convert currency amount
    .withColumn(
        "amount",
        col("amount").cast("decimal(12,2)")
    )

    # Convert source timestamps
    .withColumn(
        "payment_timestamp",
        to_timestamp(col("payment_timestamp"))
    )

    .withColumn(
        "updated_at",
        to_timestamp(col("updated_at"))
    )
)


# ---------------------------------------------------------
# 7. Keep latest version of each payment
#    within the current load_date partition
# ---------------------------------------------------------

latest_payment_window = (
    Window
    .partitionBy("payment_id")
    .orderBy(
        col("updated_at").desc(),
        col("payment_timestamp").desc()
    )
)

standardized_payments_df = (
    standardized_payments_df

    .withColumn(
        "row_num",
        row_number().over(latest_payment_window)
    )

    .filter(
        col("row_num") == 1
    )

    .drop("row_num")

    # Add Glue processing timestamp
    .withColumn(
        "standardized_at",
        current_timestamp()
    )
)


# ---------------------------------------------------------
# 8. Write standardized payments to S3
# ---------------------------------------------------------

output_path = (
    "s3://retailpulse-data-lake-saiteja/"
    "standardized/postgres/payments/"
)

(
    standardized_payments_df
    .write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("load_date")
    .parquet(output_path)
)


# ---------------------------------------------------------
# 9. Commit Glue job
# ---------------------------------------------------------

print(
    f"Successfully standardized payments "
    f"for load_date={load_date}"
)

job.commit()