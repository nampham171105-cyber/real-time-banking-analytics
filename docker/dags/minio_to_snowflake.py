import glob
import logging
import os
from datetime import datetime, timedelta

import boto3
import snowflake.connector
from airflow import DAG
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customer", "account", "transaction"]

# Dataset dùng để trigger DAG SCD2_snapshots tự động khi có dữ liệu mới,
# thay vì 2 DAG chạy theo 2 lịch cố định độc lập không liên quan nhau.
RAW_LOADED_DATASET = Dataset("snowflake://banking/raw/loaded")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def key_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    for f in glob.glob(f"{LOCAL_DIR}/*"):
        os.remove(f)

    s3 = get_s3_client()
    local_files = {}
    s3_keys_map = {}

    for table in TABLES:
        prefix = f"{table}/incoming/"
        local_files[table] = []
        s3_keys_map[table] = []
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
                    s3.download_file(BUCKET, key, local_file)
                    logger.info(f"Downloaded {key} -> {local_file}")
                    local_files[table].append(local_file)
                    s3_keys_map[table].append(key)
        except Exception:
            logger.exception(f"Failed to list/download files for table {table}")

    return {"local_files": local_files, "s3_keys_map": s3_keys_map}


def load_to_snowflake(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="download_minio")
    local_files = data["local_files"]
    s3_keys = data["s3_keys_map"]

    s3 = get_s3_client()
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )

    failed_tables = []
    any_loaded = False  # theo dõi có bảng nào thực sự load được gì không

    try:
        for table in TABLES:
            files = local_files.get(table, [])
            keys = s3_keys.get(table, [])
            if not files:
                logger.info(f"No files for {table}, skipping.")
                continue

            pending = [(f, key) for f, key in zip(files, keys) if key_exists(s3, key)]
            skipped = len(files) - len(pending)
            if skipped:
                logger.info(f"{table}: bỏ qua {skipped} file đã xử lý ở lần chạy trước.")
            if not pending:
                logger.info(f"{table}: không còn file nào cần xử lý.")
                continue

            cur = conn.cursor()
            try:
                for f, _ in pending:
                    cur.execute(f"PUT file://{f} @%{table} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
                    logger.info(f"Uploaded {f} -> @{table} stage")

                cur.execute(f"""
                    COPY INTO {table}
                    FROM @%{table}
                    FILE_FORMAT=(TYPE=PARQUET)
                    ON_ERROR='CONTINUE'
                """)
                logger.info(f"Data loaded into {table}")

                cur.execute(f"REMOVE @%{table}")
                logger.info(f"Cleared stage @%{table}")

                for _, key in pending:
                    new_key = key.replace(f"{table}/incoming/", f"{table}/processed/", 1)
                    s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=new_key)
                    s3.delete_object(Bucket=BUCKET, Key=key)
                    logger.info(f"Marked as processed: {key} -> {new_key}")

                any_loaded = True

            except Exception:
                logger.exception(f"Failed to load table {table}")
                failed_tables.append(table)
            finally:
                cur.close()
    finally:
        conn.close()

    if failed_tables:
        raise RuntimeError(f"Failed to load tables: {failed_tables}")

    if not any_loaded:
        # Không có dữ liệu mới -> skip để KHÔNG phát Dataset event,
        # nhờ đó SCD2_snapshots không bị trigger chạy dbt vô ích.
        raise AirflowSkipException("No new data loaded in this run, skipping downstream trigger.")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet (incoming/) into Snowflake RAW tables, then mark as processed/",
    schedule_interval="*/10 * * * *",   # polling mỗi 10 phút — đủ nhanh cho SLA <1h, đủ thưa để tránh small-file
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,                  # bắt buộc: chống race condition khi 2 run chồng lên nhau (COPY INTO trùng)
    tags=["minio", "snowflake", "raw"],
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        outlets=[RAW_LOADED_DATASET],
    )

    task1 >> task2