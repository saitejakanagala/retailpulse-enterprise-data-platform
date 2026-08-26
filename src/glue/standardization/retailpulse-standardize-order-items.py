import sys
import boto3

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
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

print(f"Processing order_items for load_date: {load_date}")


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
# 4. Read order_items from Glue Data Catalog
#    Process only the workflow load_date
# ---------------------------------------------------------

order_items_dynamic_frame = (
    glueContext.create_dynamic_frame.from_catalog(
        database="retailpulse_raw",
        table_name="order_items",
        push_down_predicate=f"load_date='{load_date}'"
    )
)

order_items_df = order_items_dynamic_frame.toDF()


# ---------------------------------------------------------
# 5. Stop successfully if no incremental data exists
# ---------------------------------------------------------

if order_items_df.rdd.isEmpty():
    print(
        f"No incremental order_items data found "
        f"for load_date={load_date}. Nothing to process."
    )

    job.commit()
    sys.exit(0)


# ---------------------------------------------------------
# 6. Standardize order_items data
# ---------------------------------------------------------

standardized_order_items_df = (
    order_items_df

    # Required identifiers
    .filter(
        col("order_item_id").isNotNull()
    )

    .filter(
        col("order_id").isNotNull()
    )

    .filter(
        col("product_id").isNotNull()
    )

    # Quantity must be greater than zero
    .filter(
        col("quantity") > 0
    )

    # Unit price cannot be negative
    .filter(
        col("unit_price").isNull() |
        (col("unit_price") >= 0)
    )

    # Convert source timestamps
    .withColumn(
        "created_at",
        to_timestamp(col("created_at"))
    )

    .withColumn(
        "updated_at",
        to_timestamp(col("updated_at"))
    )
)


# ---------------------------------------------------------
# 7. Keep latest version of each order item
#    within the current load_date partition
# ---------------------------------------------------------

latest_record_window = (
    Window
    .partitionBy("order_item_id")
    .orderBy(
        col("updated_at").desc(),
        col("created_at").desc()
    )
)

standardized_order_items_df = (
    standardized_order_items_df

    .withColumn(
        "row_num",
        row_number().over(latest_record_window)
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
# 8. Write standardized order_items to S3
# ---------------------------------------------------------

output_path = (
    "s3://retailpulse-data-lake-saiteja/"
    "standardized/postgres/order_items/"
)

(
    standardized_order_items_df
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
    f"Successfully standardized order_items "
    f"for load_date={load_date}"
)

job.commit()