import os
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


def get_connection():
    return psycopg.connect(
        host="127.0.0.1",
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def extract_table(table_name):
    connection = get_connection()
    cursor = connection.cursor()

    query = f"SELECT * FROM retail.{table_name};"
    cursor.execute(query)

    rows = cursor.fetchall()
    columns = [description.name for description in cursor.description]

    cursor.close()
    connection.close()

    return columns, rows


def write_csv(table_name, columns, rows):
    os.makedirs("data/extracts/postgres", exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    filename = f"{table_name}_{timestamp}.csv"

    local_path = os.path.join(
        "data",
        "extracts",
        "postgres",
        filename,
    )

    with open(local_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(columns)
        writer.writerows(rows)

    return local_path, filename


def upload_to_s3(table_name, local_path, filename):
    s3 = boto3.client("s3")

    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    s3_key = (
        f"raw/postgres/{table_name}/"
        f"load_date={load_date}/"
        f"{filename}"
    )

    s3.upload_file(
        local_path,
        S3_BUCKET,
        s3_key,
    )

    return s3_key


def main():
    for table_name in TABLES:
        print(f"\nProcessing table: {table_name}")

        columns, rows = extract_table(table_name)

        print(f"Rows extracted: {len(rows)}")

        local_path, filename = write_csv(
            table_name,
            columns,
            rows,
        )

        print(f"CSV created: {local_path}")

        s3_key = upload_to_s3(
            table_name,
            local_path,
            filename,
        )

        print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")


if __name__ == "__main__":
    main()