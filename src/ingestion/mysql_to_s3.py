import os
import io
import csv
from datetime import datetime, timezone

import boto3
import mysql.connector
from dotenv import load_dotenv


load_dotenv()


MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = os.getenv("MYSQL_PORT")

S3_BUCKET = "retailpulse-data-lake-saiteja"

TABLES = [
    "suppliers",
    "products",
    "stores",
    "inventory",
]


# ---------------------------------------------------------
# 1. Connect to MySQL
# ---------------------------------------------------------

def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        port=int(MYSQL_PORT),
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )


# ---------------------------------------------------------
# 2. Extract table data from MySQL
# ---------------------------------------------------------

def extract_table(table_name):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        f"SELECT * FROM {table_name};"
    )

    rows = cursor.fetchall()

    columns = [
        description[0]
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

    writer.writerow(columns)
    writer.writerows(rows)

    return csv_buffer.getvalue()


# ---------------------------------------------------------
# 4. Upload directly to S3
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
        f"raw/mysql/{table_name}/"
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

        columns, rows = extract_table(
            table_name
        )

        print(
            f"Rows extracted: {len(rows)}"
        )

        csv_content = create_csv_in_memory(
            columns,
            rows,
        )

        print(
            "CSV created in memory"
        )

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