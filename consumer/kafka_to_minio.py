import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import time
import os
from dotenv import load_dotenv

load_dotenv()

TOPICS = {
    'banking_server.public.customer',
    'banking_server.public.account',
    'banking_server.public.transaction'
}

# Kafka consumer settings
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),
    auto_offset_reset='earliest',
    enable_auto_commit=False,  # Tắt cơ chế tự động commit 
    group_id=os.getenv("KAFKA_GROUP"),
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
try:
    s3.head_bucket(Bucket=bucket)
except Exception:
    s3.create_bucket(Bucket=bucket)
    print(f"Created new bucket: {bucket}")

# Consume and write function
def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)

    now = datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    timestamp_str = now.strftime('%Y-%m-%d_%H%M%S_%f')

    file_name = f'{table_name}_{timestamp_str}.parquet'
    df.to_parquet(file_name, engine='fastparquet', index=False)

    s3_key = f'{table_name}/incoming/year={year}/month={month}/day={day}/{file_name}'
    s3.upload_file(file_name, bucket, s3_key)
    os.remove(file_name)
    print(f'Uploaded {len(records)} records to s3://{bucket}/{s3_key}')

buffer = {
    'banking_server.public.customer': [],
    'banking_server.public.account': [],
    'banking_server.public.transaction': []
}

print("Connected to Kafka. Listening for messages...")

BATCH_SIZE = 50
FLUSH_INTERVAL_SEC = 60
last_flush = time.time()

def flush_buffer():
    """Luôn flush TẤT CẢ topic có dữ liệu, không phân biệt đã đủ ngưỡng hay chưa.
    Nhờ vậy, ngay sau khi hàm này chạy xong, MỌI buffer đều rỗng
    -> commit() không tham số lúc này mới thực sự an toàn."""
    uploaded = False
    for topic in TOPICS:
        if len(buffer[topic]) > 0:
            write_to_minio(topic.split('.')[-1], buffer[topic])
            buffer[topic].clear()
            uploaded = True
    return uploaded


while True:
    try:
        records = consumer.poll(timeout_ms=1000)

        for tp, messages in records.items():
            topic = tp.topic
            for msg in messages:
                payload = msg.value.get("payload", {})
                op = payload.get("op")
                if op in ("c", "u", "r"):
                    tmp = payload.get("after")
                elif op == "d":
                    tmp = payload.get("before")
                tmp["_op"] = op
                tmp["_ts_ms"] = payload.get("ts_ms")
        
                buffer[topic].append(tmp)

        # Nếu topic đủ ngưỡng batch hoặc quá thời gian thì đều flush
        should_flush = False
        for topic in TOPICS:
            if len(buffer[topic]) >= BATCH_SIZE:
                should_flush = True
                break
        if time.time() - last_flush >= FLUSH_INTERVAL_SEC:
            should_flush = True
        
        if should_flush:
            uploaded = flush_buffer()   # flush HẾT mọi topic, không chừa lại gì

            if uploaded:
                consumer.commit()        # commit khi mọi buffer đều đã rỗng
                print("Offsets committed")

            last_flush = time.time()

    except Exception as e:
        print("=" * 60)
        print("ERROR:", e)
        print("Skip commit. Kafka will resend this batch.")
        print("=" * 60)
        time.sleep(2)