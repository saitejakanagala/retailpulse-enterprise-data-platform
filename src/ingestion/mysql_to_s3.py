import os
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


def get_connection():
    return mysql.connector.connect(
        host="127.0.0.1",
        port=int(MYSQL_PORT),
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )


def extract_table(table_name):
    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(f"SELECT * FROM {table_name};")

    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]

    cursor.close()
    connection.close()

    return columns, rows


def write_csv(table_name, columns, rows):
    os.makedirs("data/extracts/mysql", exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    filename = f"{table_name}_{timestamp}.csv"

    local_path = os.path.join(
        "data",
        "extracts",
        "mysql",
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
        f"raw/mysql/{table_name}/"
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