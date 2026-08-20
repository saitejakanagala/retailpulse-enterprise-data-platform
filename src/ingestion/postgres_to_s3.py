import os
import io
import csv
from datetime import datetime, timezone

import boto3
import psycopg
from dotenv import load_dotenv


load_dotenv()


POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

S3_BUCKET = "retailpulse-data-lake-saiteja"

TABLES = [
    "customers",
    "orders",
    "order_items",
    "payments",
]


# ---------------------------------------------------------
# 1. Connect to PostgreSQL
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
# 2. Extract table data from PostgreSQL
# ---------------------------------------------------------

def extract_table(table_name):
    connection = get_connection()
    cursor = connection.cursor()

    query = f"SELECT * FROM retail.{table_name};"

    cursor.execute(query)

    rows = cursor.fetchall()

    columns = [
        description.name
        for description in cursor.description
    ]

    cursor.close()
    connection.close()

    return columns, rows


# ---------------------------------------------------------
# 3. Create CSV in memory
# ---------------------------------------------------------

def create_csv_in_memory(columns, rows):

    csv_buffer = io.StringIO()

    writer = csv.writer(csv_buffer)

    # Write CSV header
    writer.writerow(columns)

    # Write all database rows
    writer.writerows(rows)

    return csv_buffer.getvalue()


# ---------------------------------------------------------
# 4. Upload CSV directly from memory to S3
# ---------------------------------------------------------

def upload_to_s3(
    table_name,
    csv_content,
):

    s3 = boto3.client(
        "s3",
        region_name="us-east-2",
    )

    load_date = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    timestamp = datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")

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
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )

    return s3_key


# ---------------------------------------------------------
# 5. Main ingestion process
# ---------------------------------------------------------

def main():

    for table_name in TABLES:

        print(
            f"\nProcessing table: {table_name}"
        )

        # Extract PostgreSQL data
        columns, rows = extract_table(
            table_name
        )

        print(
            f"Rows extracted: {len(rows)}"
        )

        # Create CSV in memory
        csv_content = create_csv_in_memory(
            columns,
            rows,
        )

        print(
            "CSV created in memory"
        )

        # Upload directly to S3
        s3_key = upload_to_s3(
            table_name,
            csv_content,
        )

        print(
            f"Uploaded to "
            f"s3://{S3_BUCKET}/{s3_key}"
        )


if __name__ == "__main__":
    main()