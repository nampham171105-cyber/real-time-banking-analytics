import os
import glob
import logging
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customer", "account", "transaction"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def key_exists(s3, key: str) -> bool:
    """Kiểm tra 1 object còn tồn tại trên MinIO hay không."""
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


# -------- Task 1: liệt kê + tải file từ incoming/ --------
def download_from_minio():
    """
    Liệt kê và tải toàn bộ file trong `<table>/incoming/`.
    Dùng paginator vì list_objects_v2 chỉ trả tối đa 1000 object/lần.
    """
    os.makedirs(LOCAL_DIR, exist_ok=True)

    # Dọn sạch thư mục tạm trước khi tải file mới
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
            # Lỗi khi list/download 1 bảng không nên chặn việc tải các bảng khác
            logger.error(f"Failed to list/download files for table {table}", exc_info=True)

    return {"local_files": local_files, "s3_keys_map": s3_keys_map}


# -------- Task 2: load vào Snowflake, chống trùng lặp khi retry --------
def load_to_snowflake(**kwargs):
    """
    Load file Parquet vào Snowflake, sau đó move file đã xử lý sang `<table>/processed/`.

    CHỐNG TRÙNG LẶP KHI RETRY:
    Trước khi xử lý mỗi file, kiểm tra lại trên MinIO xem key đó còn nằm ở
    `incoming/` hay không.
      - Còn tồn tại  -> chưa xử lý (hoặc lần trước lỗi giữa chừng) -> xử lý.
      - Không còn nữa -> lần chạy trước đã COPY INTO + move xong rồi -> bỏ qua.
    Nhờ vậy, dù Airflow retry bao nhiêu lần, mỗi file chỉ thực sự được
    COPY INTO đúng 1 lần — không cần lưu thêm trạng thái ở đâu khác, MinIO
    tự đóng vai trò "nguồn sự thật" cho việc file nào đã xử lý xong.

    Mỗi bảng xử lý độc lập: bảng này lỗi không chặn các bảng khác.
    """
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

    try:
        for table in TABLES:
            files = local_files.get(table, [])
            keys = s3_keys.get(table, [])
            if not files:
                logger.info(f"No files for {table}, skipping.")
                continue

            # Chỉ giữ lại các cặp (file local, key) mà key VẪN CÒN trên MinIO.
            # Đây là bước chống trùng lặp cốt lõi khi retry.
            pending = [
                (f, key) for f, key in zip(files, keys) if key_exists(s3, key)
            ]
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

                # Chỉ move file khi COPY INTO của đúng bảng này đã thành công
                for _, key in pending:
                    new_key = key.replace(f"{table}/incoming/", f"{table}/processed/", 1)
                    s3.copy_object(
                        Bucket=BUCKET,
                        CopySource={"Bucket": BUCKET, "Key": key},
                        Key=new_key,
                    )
                    s3.delete_object(Bucket=BUCKET, Key=key)
                    logger.info(f"Marked as processed: {key} -> {new_key}")

            except Exception:
                logger.error(f"Failed to load table {table}", exc_info=True)
                failed_tables.append(table)
            finally:
                cur.close()

    finally:
        conn.close()

    if failed_tables:
        raise RuntimeError(f"Failed to load tables: {failed_tables}")


# -------- Airflow DAG --------
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
    #schedule_interval="*/10 * * * *",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    #max_active_runs=1, 
    tags=["minio", "snowflake", "raw"],
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    task1 >> task2