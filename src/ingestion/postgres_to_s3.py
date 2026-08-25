import os
import io
import csv
from datetime import datetime, timezone

import boto3
import psycopg
from dotenv import load_dotenv


load_dotenv()


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

AWS_REGION = os.getenv(
    "AWS_REGION",
    "us-east-2",
)

S3_BUCKET = os.getenv(
    "S3_BUCKET",
    "retailpulse-data-lake-saiteja",
)

WATERMARK_TABLE = os.getenv(
    "WATERMARK_TABLE",
    "retailpulse-watermarks",
)


TABLES = [
    "customers",
    "orders",
    "order_items",
    "payments",
]


# Each PostgreSQL table uses:
# updated_at + primary-key ID
# as a composite watermark.
WATERMARK_ID_COLUMNS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}


# ---------------------------------------------------------
# AWS clients
# ---------------------------------------------------------

dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
)

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
)


# ---------------------------------------------------------
# 1. Read composite watermark from DynamoDB
# ---------------------------------------------------------

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

    # PostgreSQL uses TIMESTAMP WITHOUT TIME ZONE.
    # Convert DynamoDB string back to Python datetime
    # before sending it to PostgreSQL.
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


# ---------------------------------------------------------
# 2. Determine new composite watermark
# ---------------------------------------------------------

def get_new_watermark(
    table_name,
    columns,
    rows,
):

    if not rows:
        return None

    id_column = WATERMARK_ID_COLUMNS[
        table_name
    ]

    id_index = columns.index(
        id_column
    )

    updated_at_index = columns.index(
        "updated_at"
    )

    # SQL results are ordered by:
    # updated_at, primary-key ID.
    #
    # Therefore the final row represents
    # the newest composite watermark.
    last_row = rows[-1]

    new_updated_at = last_row[
        updated_at_index
    ]

    new_id = last_row[
        id_index
    ]

    return new_updated_at, new_id


# ---------------------------------------------------------
# 3. Update composite watermark in DynamoDB
# ---------------------------------------------------------

def update_watermark(
    table_name,
    new_updated_at,
    new_id,
):

    source_table = f"postgres#{table_name}"

    if isinstance(
        new_updated_at,
        datetime,
    ):
        updated_at_string = (
            new_updated_at.strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
        )

    else:
        updated_at_string = str(
            new_updated_at
        )

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

    print(
        f"Watermark updated for {table_name}: "
        f"updated_at={updated_at_string}, "
        f"id={new_id}"
    )


# ---------------------------------------------------------
# 4. Connect to PostgreSQL
# ---------------------------------------------------------

def get_connection():

    return psycopg.connect(
        host="127.0.0.1",
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ---------------------------------------------------------
# 5. Extract incremental table data
# ---------------------------------------------------------

def extract_table(table_name):

    if table_name not in WATERMARK_ID_COLUMNS:
        raise ValueError(
            f"No watermark configuration "
            f"for table: {table_name}"
        )

    id_column = WATERMARK_ID_COLUMNS[
        table_name
    ]

    last_updated_at, last_id = (
        get_watermark(
            table_name
        )
    )

    print(
        f"Current watermark for {table_name}: "
        f"updated_at={last_updated_at}, "
        f"id={last_id}"
    )

    connection = get_connection()
    cursor = connection.cursor()

    # Composite watermark:
    #
    # 1. Any row with a newer updated_at is selected.
    #
    # 2. If multiple rows have the same updated_at,
    #    the primary-key ID acts as the tie-breaker.
    query = f"""
        SELECT *
        FROM retail.{table_name}
        WHERE updated_at > %s
           OR (
               updated_at = %s
               AND {id_column} > %s
           )
        ORDER BY updated_at, {id_column};
    """

    try:
        cursor.execute(
            query,
            (
                last_updated_at,
                last_updated_at,
                last_id,
            ),
        )

        rows = cursor.fetchall()

        columns = [
            description.name
            for description
            in cursor.description
        ]

    finally:
        cursor.close()
        connection.close()

    return columns, rows


# ---------------------------------------------------------
# 6. Create CSV in memory
# ---------------------------------------------------------

def create_csv_in_memory(
    columns,
    rows,
):

    csv_buffer = io.StringIO()

    writer = csv.writer(
        csv_buffer
    )

    writer.writerow(
        columns
    )

    writer.writerows(
        rows
    )

    return csv_buffer.getvalue()


# ---------------------------------------------------------
# 7. Upload incremental batch directly to S3
# ---------------------------------------------------------

def upload_to_s3(
    table_name,
    csv_content,
):

    current_time = datetime.now(
        timezone.utc
    )

    load_date = current_time.strftime(
        "%Y-%m-%d"
    )

    timestamp = current_time.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    filename = (
        f"{table_name}_"
        f"{timestamp}.csv"
    )

    s3_key = (
        f"raw/postgres/{table_name}/"
        f"load_date={load_date}/"
        f"{filename}"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_content.encode(
            "utf-8"
        ),
        ContentType="text/csv",
    )

    return s3_key


# ---------------------------------------------------------
# 8. Main incremental ingestion process
# ---------------------------------------------------------

def main():

    for table_name in TABLES:

        print(
            "\n"
            "----------------------------------------"
        )

        print(
            f"Processing table: {table_name}"
        )

        print(
            "----------------------------------------"
        )

        # Read only records newer than
        # the DynamoDB composite watermark.
        columns, rows = extract_table(
            table_name
        )

        print(
            f"Rows extracted: {len(rows)}"
        )

        # Do not create empty S3 objects.
        if not rows:

            print(
                f"No new or updated rows for "
                f"{table_name}. "
                f"Skipping S3 upload."
            )

            continue

        # Determine newest:
        # (updated_at, primary_key)
        new_watermark = get_new_watermark(
            table_name,
            columns,
            rows,
        )

        new_updated_at, new_id = (
            new_watermark
        )

        print(
            f"New watermark candidate: "
            f"updated_at={new_updated_at}, "
            f"id={new_id}"
        )

        # Create CSV only in memory.
        csv_content = create_csv_in_memory(
            columns,
            rows,
        )

        print(
            "CSV created in memory"
        )

        # Upload data before advancing
        # the DynamoDB checkpoint.
        s3_key = upload_to_s3(
            table_name,
            csv_content,
        )

        print(
            f"Uploaded to "
            f"s3://{S3_BUCKET}/{s3_key}"
        )

        # IMPORTANT:
        # DynamoDB advances only after
        # the S3 upload succeeds.
        update_watermark(
            table_name,
            new_updated_at,
            new_id,
        )

    print(
        "\nPostgreSQL incremental ingestion completed."
    )


if __name__ == "__main__":
    main()