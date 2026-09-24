import csv
import io
import os
from datetime import datetime, timezone

import boto3
import mysql.connector
from dotenv import load_dotenv


# ---------------------------------------------------------
# Environment
# ---------------------------------------------------------

load_dotenv()

MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3307"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


# ---------------------------------------------------------
# AWS configuration
# ---------------------------------------------------------

AWS_REGION = "us-east-2"

S3_BUCKET = "retailpulse-data-lake-saiteja"

WATERMARK_TABLE = "retailpulse-watermarks"


# ---------------------------------------------------------
# MySQL table configuration
# ---------------------------------------------------------

TABLE_CONFIG = {
    "suppliers": {
        "primary_key": "supplier_id",
    },
    "products": {
        "primary_key": "product_id",
    },
    "stores": {
        "primary_key": "store_id",
    },
    "inventory": {
        "primary_key": "inventory_id",
    },
}


# ---------------------------------------------------------
# AWS clients
# ---------------------------------------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
)

dynamodb = boto3.client(
    "dynamodb",
    region_name=AWS_REGION,
)


# ---------------------------------------------------------
# MySQL connection
# ---------------------------------------------------------

def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )


# ---------------------------------------------------------
# DynamoDB watermark key
# ---------------------------------------------------------

def get_watermark_key(table_name):
    return f"mysql#{table_name}"


# ---------------------------------------------------------
# Read watermark
# ---------------------------------------------------------

def get_watermark(table_name):
    source_table = get_watermark_key(
        table_name
    )

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
            f"No watermark found for "
            f"{source_table}. "
            f"Incremental ingestion stopped "
            f"to prevent accidental full load."
        )

    if (
        "last_updated_at" not in item
        or "last_id" not in item
    ):
        raise RuntimeError(
            f"Watermark for {source_table} "
            f"is incomplete."
        )

    last_updated_at = (
        item["last_updated_at"]["S"]
    )

    last_id = int(
        item["last_id"]["N"]
    )

    return last_updated_at, last_id


# ---------------------------------------------------------
# Update watermark
# ---------------------------------------------------------

def update_watermark(
    table_name,
    last_updated_at,
    last_id,
):
    source_table = get_watermark_key(
        table_name
    )

    dynamodb.update_item(
        TableName=WATERMARK_TABLE,
        Key={
            "source_table": {
                "S": source_table
            }
        },
        UpdateExpression=(
            "SET last_updated_at = :ts, "
            "last_id = :id"
        ),
        ExpressionAttributeValues={
            ":ts": {
                "S": last_updated_at
            },
            ":id": {
                "N": str(last_id)
            },
        },
    )

    print(
        f"Watermark updated for "
        f"{table_name}: "
        f"updated_at={last_updated_at}, "
        f"id={last_id}"
    )


# ---------------------------------------------------------
# Extract incremental rows
# ---------------------------------------------------------

def extract_table(table_name):
    primary_key = (
        TABLE_CONFIG[table_name][
            "primary_key"
        ]
    )

    (
        last_updated_at,
        last_id,
    ) = get_watermark(
        table_name
    )

    print(
        f"Current watermark for "
        f"{table_name}: "
        f"updated_at={last_updated_at}, "
        f"id={last_id}"
    )

    connection = get_mysql_connection()

    try:
        cursor = connection.cursor()

        query = f"""
            SELECT *
            FROM {table_name}
            WHERE
                updated_at > %s
                OR (
                    updated_at = %s
                    AND {primary_key} > %s
                )
            ORDER BY
                updated_at ASC,
                {primary_key} ASC;
        """

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
            description[0]
            for description
            in cursor.description
        ]

        print(
            f"Rows extracted: "
            f"{len(rows)}"
        )

        if not rows:
            return (
                columns,
                [],
                None,
                None,
            )

        updated_at_index = (
            columns.index("updated_at")
        )

        primary_key_index = (
            columns.index(primary_key)
        )

        last_row = rows[-1]

        new_updated_at = (
            last_row[
                updated_at_index
            ]
        )

        new_id = int(
            last_row[
                primary_key_index
            ]
        )

        if isinstance(
            new_updated_at,
            datetime,
        ):
            new_updated_at = (
                new_updated_at.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )
        else:
            new_updated_at = str(
                new_updated_at
            )

        print(
            f"New watermark candidate: "
            f"updated_at="
            f"{new_updated_at}, "
            f"id={new_id}"
        )

        return (
            columns,
            rows,
            new_updated_at,
            new_id,
        )

    finally:
        cursor.close()
        connection.close()


# ---------------------------------------------------------
# Convert rows to CSV
# ---------------------------------------------------------

def create_csv(
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

    print(
        "CSV created in memory"
    )

    return csv_buffer.getvalue()


# ---------------------------------------------------------
# Upload CSV to S3
# ---------------------------------------------------------

def upload_to_s3(
    table_name,
    csv_content,
    load_date,
):
    now_utc = datetime.now(
        timezone.utc
    )

    timestamp = now_utc.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    file_name = (
        f"{table_name}_"
        f"{timestamp}.csv"
    )

    s3_key = (
        f"raw/mysql/"
        f"{table_name}/"
        f"load_date={load_date}/"
        f"{file_name}"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_content.encode(
            "utf-8"
        ),
        ContentType="text/csv",
    )

    print(
        f"Uploaded to "
        f"s3://{S3_BUCKET}/"
        f"{s3_key}"
    )

    return s3_key


# ---------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------

def main():
    load_date = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d"
    )

    print()
    print(
        "RetailPulse MySQL "
        "incremental ingestion"
    )
    print(
        "================================"
    )

    for table_name in TABLE_CONFIG:

        print()
        print(
            "----------------------------------------"
        )
        print(
            f"Processing table: "
            f"{table_name}"
        )
        print(
            "----------------------------------------"
        )

        (
            columns,
            rows,
            new_updated_at,
            new_id,
        ) = extract_table(
            table_name
        )

        if not rows:
            print(
                f"No new or updated rows "
                f"for {table_name}. "
                f"Skipping S3 upload."
            )

            continue

        csv_content = create_csv(
            columns,
            rows,
        )

        # -------------------------------------------------
        # Upload first
        # -------------------------------------------------
        #
        # The watermark MUST NOT advance until
        # the raw file has successfully reached S3.
        # -------------------------------------------------

        upload_to_s3(
            table_name,
            csv_content,
            load_date,
        )

        # -------------------------------------------------
        # Update watermark only after successful S3 upload
        # -------------------------------------------------

        update_watermark(
            table_name,
            new_updated_at,
            new_id,
        )

    print()
    print(
        "MySQL incremental ingestion completed."
    )


if __name__ == "__main__":
    main()