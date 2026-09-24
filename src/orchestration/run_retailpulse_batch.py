import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

AWS_REGION = "us-east-2"

S3_BUCKET = "retailpulse-data-lake-saiteja"

CONTROL_PREFIX = "control/ingestion_batches/"


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

# Current file:
# src/orchestration/run_retailpulse_batch.py
#
# parents[2] brings us back to the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[2]


# Use the project's virtual-environment Python.
PYTHON_EXECUTABLE = (
    PROJECT_ROOT
    / ".venv"
    / "Scripts"
    / "python.exe"
)


# ---------------------------------------------------------
# Existing RetailPulse scripts
# ---------------------------------------------------------

POSTGRES_GENERATOR = (
    PROJECT_ROOT
    / "src"
    / "data_generator"
    / "generate_postgres_data.py"
)

MYSQL_GENERATOR = (
    PROJECT_ROOT
    / "src"
    / "data_generator"
    / "generate_mysql_data.py"
)

POSTGRES_INGESTION = (
    PROJECT_ROOT
    / "src"
    / "ingestion"
    / "postgres_to_s3.py"
)

MYSQL_INGESTION = (
    PROJECT_ROOT
    / "src"
    / "ingestion"
    / "mysql_to_s3.py"
)


# ---------------------------------------------------------
# AWS client
# ---------------------------------------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
)


# ---------------------------------------------------------
# Check for unresolved previous batches
# ---------------------------------------------------------

def get_pending_markers():
    """
    Look inside the S3 control folder.

    Any JSON marker that still exists is treated as an
    unresolved pipeline batch.

    If a marker exists, we do NOT generate or ingest
    another batch.
    """

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    pages = paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=CONTROL_PREFIX,
    )

    pending_markers = []

    for page in pages:

        for obj in page.get(
            "Contents",
            [],
        ):

            key = obj["Key"]

            if key.endswith(".json"):
                pending_markers.append(
                    key
                )

    # Oldest marker first.
    pending_markers.sort()

    return pending_markers


# ---------------------------------------------------------
# Execute one child Python script
# ---------------------------------------------------------

def run_script(
    script_path,
    description,
):
    """
    Execute one of the existing RetailPulse scripts.

    If the child script fails, stop the whole batch.
    """

    print()
    print("=" * 70)
    print(description)
    print("=" * 70)

    result = subprocess.run(
        [
            str(PYTHON_EXECUTABLE),
            str(script_path),
        ],
        cwd=str(PROJECT_ROOT),
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"{description} failed "
            f"with exit code "
            f"{result.returncode}"
        )

    print()
    print(
        f"{description} "
        f"completed successfully."
    )


# ---------------------------------------------------------
# Find RAW files produced during current batch
# ---------------------------------------------------------

def find_batch_raw_objects(
    load_date,
    batch_started_at,
):
    """
    Find PostgreSQL and MySQL RAW files created after
    this particular orchestration batch started.

    This prevents files from an older run on the same
    load_date from being counted as part of this batch.
    """

    raw_prefixes = [
        (
            "postgres",
            "raw/postgres/",
        ),
        (
            "mysql",
            "raw/mysql/",
        ),
    ]

    partition_text = (
        f"load_date={load_date}/"
    )

    raw_objects = []

    for (
        source_name,
        source_prefix,
    ) in raw_prefixes:

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        pages = paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=source_prefix,
        )

        for page in pages:

            for obj in page.get(
                "Contents",
                [],
            ):

                key = obj["Key"]

                # Ignore other load_date partitions.
                if (
                    partition_text
                    not in key
                ):
                    continue

                # Ignore files created before this
                # orchestration batch started.
                if (
                    obj["LastModified"]
                    < batch_started_at
                ):
                    continue

                raw_objects.append(
                    {
                        "source": (
                            source_name
                        ),
                        "key": key,
                        "size_bytes": (
                            obj["Size"]
                        ),
                    }
                )

    return raw_objects


# ---------------------------------------------------------
# Create a batch marker
# ---------------------------------------------------------

def create_batch_marker(
    batch_id,
    load_date,
    status,
    raw_objects,
):
    """
    Create the control JSON after ingestion succeeds.

    DATA_AVAILABLE means RAW data is ready for Glue.

    NO_DATA means ingestion completed successfully,
    but no RAW files were created.
    """

    completed_at = datetime.now(
        timezone.utc
    )

    marker = {
        "batch_id": batch_id,
        "load_date": load_date,
        "status": status,
        "raw_file_count": len(
            raw_objects
        ),
        "raw_objects": raw_objects,
        "completed_at": (
            completed_at.isoformat()
        ),
    }

    marker_key = (
        f"{CONTROL_PREFIX}"
        f"load_date={load_date}/"
        f"{batch_id}_{status}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=marker_key,
        Body=json.dumps(
            marker,
            indent=2,
        ).encode(
            "utf-8"
        ),
        ContentType=(
            "application/json"
        ),
    )

    print()
    print(
        "Batch marker created:"
    )

    print(
        f"s3://{S3_BUCKET}/"
        f"{marker_key}"
    )

    return marker_key


# ---------------------------------------------------------
# Create a FAILED marker
# ---------------------------------------------------------

def create_failed_marker(
    batch_id,
    load_date,
    error_message,
):
    """
    If the orchestration itself fails, create a FAILED
    marker.

    This is important because the next orchestration run
    will see the FAILED marker and stop instead of adding
    another batch on top of a partially failed batch.
    """

    marker = {
        "batch_id": batch_id,
        "load_date": load_date,
        "status": "FAILED",
        "error": error_message,
        "failed_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
    }

    marker_key = (
        f"{CONTROL_PREFIX}"
        f"load_date={load_date}/"
        f"{batch_id}_FAILED.json"
    )

    try:

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=marker_key,
            Body=json.dumps(
                marker,
                indent=2,
            ).encode(
                "utf-8"
            ),
            ContentType=(
                "application/json"
            ),
        )

        print()
        print(
            "FAILED batch marker created:"
        )

        print(
            f"s3://{S3_BUCKET}/"
            f"{marker_key}"
        )

    except Exception as marker_error:

        print()
        print(
            "WARNING: Could not create "
            "FAILED batch marker."
        )

        print(
            f"Marker error: "
            f"{marker_error}"
        )


# ---------------------------------------------------------
# Main RetailPulse orchestration
# ---------------------------------------------------------

def main():

    print()
    print(
        "RetailPulse pipeline control check"
    )
    print(
        "=================================="
    )

    # -----------------------------------------------------
    # STEP 0
    # Check whether an older batch is unresolved.
    # -----------------------------------------------------

    pending_markers = (
        get_pending_markers()
    )

    if pending_markers:

        print()
        print(
            "Previous pipeline batch "
            "is still unresolved."
        )

        print()
        print(
            "New source generation and "
            "RAW ingestion will NOT start."
        )

        print()
        print(
            "Pending control marker(s):"
        )

        for marker in pending_markers:

            print(
                f"  {marker}"
            )

        print()
        print(
            "The previous Glue workflow "
            "must succeed and its marker "
            "must be cleared before another "
            "batch can start."
        )

        sys.exit(1)

    # -----------------------------------------------------
    # No existing marker.
    # Safe to begin a new batch.
    # -----------------------------------------------------

    batch_started_at = datetime.now(
        timezone.utc
    )

    load_date = (
        batch_started_at.strftime(
            "%Y-%m-%d"
        )
    )

    batch_id = (
        batch_started_at.strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    print()
    print(
        "No unresolved batch found."
    )

    print(
        "Starting new RetailPulse batch."
    )

    print()
    print(
        "=================================="
    )

    print(
        "RetailPulse automated batch started"
    )

    print(
        "=================================="
    )

    print(
        f"Batch ID  : {batch_id}"
    )

    print(
        f"Load date : {load_date}"
    )

    try:

        # -------------------------------------------------
        # STEP 1
        # Generate PostgreSQL source activity
        # -------------------------------------------------

        run_script(
            POSTGRES_GENERATOR,
            "PostgreSQL source generation",
        )

        # -------------------------------------------------
        # STEP 2
        # Generate MySQL source activity
        # -------------------------------------------------

        run_script(
            MYSQL_GENERATOR,
            "MySQL source generation",
        )

        # -------------------------------------------------
        # STEP 3
        # Incremental PostgreSQL → S3
        # -------------------------------------------------

        run_script(
            POSTGRES_INGESTION,
            (
                "PostgreSQL "
                "incremental ingestion"
            ),
        )

        # -------------------------------------------------
        # STEP 4
        # Incremental MySQL → S3
        # -------------------------------------------------

        run_script(
            MYSQL_INGESTION,
            (
                "MySQL "
                "incremental ingestion"
            ),
        )

        # -------------------------------------------------
        # STEP 5
        # Find RAW files belonging to this batch
        # -------------------------------------------------

        raw_objects = (
            find_batch_raw_objects(
                load_date,
                batch_started_at,
            )
        )

        print()
        print(
            f"RAW files created "
            f"during this batch: "
            f"{len(raw_objects)}"
        )

        for obj in raw_objects:

            print(
                f"  {obj['source']}: "
                f"{obj['key']}"
            )

        # -------------------------------------------------
        # STEP 6
        # Determine batch status
        # -------------------------------------------------

        if raw_objects:

            status = (
                "DATA_AVAILABLE"
            )

        else:

            status = (
                "NO_DATA"
            )

        # -------------------------------------------------
        # STEP 7
        # Create control marker
        # -------------------------------------------------

        marker_key = (
            create_batch_marker(
                batch_id,
                load_date,
                status,
                raw_objects,
            )
        )

        # -------------------------------------------------
        # Final summary
        # -------------------------------------------------

        print()
        print(
            "=================================="
        )

        print(
            "RetailPulse batch completed"
        )

        print(
            "=================================="
        )

        print(
            f"Batch ID  : {batch_id}"
        )

        print(
            f"Load date : {load_date}"
        )

        print(
            f"Status    : {status}"
        )

        print(
            f"RAW files : "
            f"{len(raw_objects)}"
        )

        print(
            f"Marker    : "
            f"s3://{S3_BUCKET}/"
            f"{marker_key}"
        )

        if status == "DATA_AVAILABLE":

            print()
            print(
                "RAW ingestion is complete."
            )

            print(
                "The control marker is now "
                "waiting for AWS Lambda / "
                "Glue processing."
            )

    except Exception as exc:

        error_message = str(
            exc
        )

        print()
        print(
            "=================================="
        )

        print(
            "RetailPulse batch FAILED"
        )

        print(
            "=================================="
        )

        print(
            f"Batch ID  : {batch_id}"
        )

        print(
            f"Load date : {load_date}"
        )

        print(
            f"Error     : "
            f"{error_message}"
        )

        # -------------------------------------------------
        # Create FAILED control marker.
        #
        # This prevents another orchestration run from
        # putting more RAW files into the pipeline until
        # the failure has been investigated.
        # -------------------------------------------------

        create_failed_marker(
            batch_id,
            load_date,
            error_message,
        )

        sys.exit(1)


# ---------------------------------------------------------
# Program entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    main()
    