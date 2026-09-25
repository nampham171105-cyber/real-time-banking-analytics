import json
import os
import time
import traceback
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

TOPICS = {
    'banking_server.public.customer',
    'banking_server.public.account',
    'banking_server.public.transaction'
}

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

try:
    s3.head_bucket(Bucket=bucket)
except Exception:
    s3.create_bucket(Bucket=bucket)
    print(f"Created new bucket: {bucket}")


def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)

    now = datetime.now()
    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    timestamp_str = now.strftime('%Y-%m-%d_%H%M%S_%f')

    file_name = f'{table_name}_{timestamp_str}.parquet'
    df.to_parquet(file_name, engine='fastparquet', index=False)

    s3_key = f'{table_name}/incoming/year={year}/month={month}/day={day}/{file_name}'
    s3.upload_file(file_name, bucket, s3_key)
    os.remove(file_name)
    print(f'Uploaded {len(records)} records to s3://{bucket}/{s3_key}')


def write_to_dlq(topic, raw_message, error):
    """Ghi message không xử lý được vào DLQ trên MinIO thay vì làm treo consumer.
    Mỗi message lỗi được lưu riêng kèm lý do lỗi để dễ điều tra/replay sau này."""
    now = datetime.now()
    year, month, day = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    timestamp_str = now.strftime('%Y-%m-%d_%H%M%S_%f')

    table_name = topic.split('.')[-1]
    dlq_record = {
        "topic": topic,
        "error": str(error),
        "traceback": traceback.format_exc(),
        "raw_value": json.dumps(raw_message, default=str),
        "failed_at": now.isoformat(),
    }

    file_name = f'dlq_{table_name}_{timestamp_str}.json'
    with open(file_name, 'w') as f:
        json.dump(dlq_record, f)

    s3_key = f'_dlq/{table_name}/year={year}/month={month}/day={day}/{file_name}'
    s3.upload_file(file_name, bucket, s3_key)
    os.remove(file_name)
    print(f'[DLQ] Wrote poison message to s3://{bucket}/{s3_key} — {error}')


buffer = {
    'banking_server.public.customer': [],
    'banking_server.public.account': [],
    'banking_server.public.transaction': []
}

print("Connected to Kafka. Listening for messages...")

# Tăng batch size để giảm số file nhỏ ghi lên MinIO -> giảm số lần
# COPY INTO nhỏ lẻ ở phía Snowflake (giảm chi phí compute cố định/lần load).
BATCH_SIZE = 5000
FLUSH_INTERVAL_SEC = 120
last_flush = time.time()


def flush_buffer():
    uploaded = False
    for topic in TOPICS:
        if len(buffer[topic]) > 0:
            write_to_minio(topic.split('.')[-1], buffer[topic])
            buffer[topic].clear()
            uploaded = True
    return uploaded


# Số lần lỗi liên tiếp cho phép trước khi backoff dài hơn (bảo vệ khi
# Kafka/MinIO down kéo dài, tránh spam retry vô tội vạ).
consecutive_errors = 0
MAX_BACKOFF_SEC = 60

while True:
    try:
        records = consumer.poll(timeout_ms=1000)
        consecutive_errors = 0  # poll thành công -> reset backoff

        for tp, messages in records.items():
            topic = tp.topic
            for msg in messages:
                try:
                    payload = msg.value.get("payload", {})
                    op = payload.get("op")
                    if op in ("c", "u", "r"):
                        tmp = payload.get("after")
                    elif op == "d":
                        tmp = payload.get("before")
                    else:
                        raise ValueError(f"Unknown op type: {op}")

                    if tmp is None:
                        raise ValueError("Payload before/after is None — có thể do REPLICA IDENTITY chưa bật FULL")

                    tmp["_op"] = op
                    tmp["_ts_ms"] = payload.get("ts_ms")
                    buffer[topic].append(tmp)

                except Exception as msg_err:
                    # Poison message: không để 1 message lỗi làm treo toàn bộ consumer.
                    # Ghi vào DLQ, log lỗi, rồi tiếp tục xử lý các message khác trong batch.
                    write_to_dlq(topic, msg.value, msg_err)

        should_flush = any(len(buffer[t]) >= BATCH_SIZE for t in TOPICS)
        if time.time() - last_flush >= FLUSH_INTERVAL_SEC:
            should_flush = True

        if should_flush:
            uploaded = flush_buffer()
            if uploaded:
                consumer.commit()
                print("Offsets committed")
            last_flush = time.time()

    except Exception as e:
        # Lỗi hệ thống (Kafka/MinIO down...) -> backoff tăng dần thay vì
        # sleep(2) cố định, tránh spam retry khi outage kéo dài.
        consecutive_errors += 1
        backoff = min(2 ** consecutive_errors, MAX_BACKOFF_SEC)
        print("=" * 60)
        print("ERROR:", e)
        print(f"Skip commit. Retrying in {backoff}s (consecutive errors: {consecutive_errors})")
        print("=" * 60)
        time.sleep(backoff)