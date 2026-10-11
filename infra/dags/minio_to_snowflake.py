"""Manual Airflow DAG: fetch paginated SeaweedFS S3 JSON into Snowflake."""
import os
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET = os.getenv("S3_BUCKET", "bronze-transactions")
LOCAL_DIR = Path("/tmp/minio_downloads")
TABLE = "BRONZE_STOCK_QUOTES_RAW"


def download_from_minio():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:8333"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    paginator = s3.get_paginator("list_objects_v2")
    paths, symbols, pages = [], set(), 0
    for page in paginator.paginate(Bucket=BUCKET):
        pages += 1
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            # The producer writes SYMBOL/timestamp_uuid.json only.
            safe_name = key.replace("/", "_")
            dest = LOCAL_DIR / safe_name
            s3.download_file(BUCKET, key, str(dest))
            paths.append(str(dest))
            symbols.add(key.split("/")[0])
    print(f"S3 pages: {pages}; files: {len(paths)}; symbols: {sorted(symbols)}")
    return paths


def load_to_snowflake(**context):
    files = context["ti"].xcom_pull(task_ids="download_minio") or []
    if not files:
        print("No JSON files available to load.")
        return
    connection_args = {
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "database": os.getenv("SNOWFLAKE_DB", "STOCKS_DBM"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "COMMON"),
    }
    if os.getenv("SNOWFLAKE_ROLE"):
        connection_args["role"] = os.environ["SNOWFLAKE_ROLE"]
    conn = snowflake.connector.connect(**connection_args)
    try:
        cur = conn.cursor()
        try:
            for file_path in files:
                # Filenames are generated internally and contain no spaces.
                cur.execute(
                    f"PUT file://{file_path} @%{TABLE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
            print(f"Uploaded {len(files)} files to Snowflake table stage")
            cur.execute(
                f"COPY INTO {TABLE} FROM @%{TABLE} FILE_FORMAT = (TYPE = JSON)"
            )
            for row in cur.fetchall():
                print(f"COPY result: {row}")
        finally:
            cur.close()
    finally:
        conn.close()


with DAG(
    dag_id="minio_to_snowflake",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2026, 10, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "bronze", "stocks"],
) as dag:
    download_minio = PythonOperator(
        task_id="download_minio", python_callable=download_from_minio
    )
    load_snowflake = PythonOperator(
        task_id="load_snowflake", python_callable=load_to_snowflake
    )
    download_minio >> load_snowflake
