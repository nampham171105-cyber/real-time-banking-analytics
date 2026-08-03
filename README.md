# Banking Real-Time Data Pipeline (CDC → Kafka → MinIO → Snowflake → dbt)

Một data pipeline end-to-end mô phỏng hệ thống ngân hàng, capture thay đổi dữ liệu theo thời gian thực từ Postgres bằng Debezium (CDC), đẩy qua Kafka, lưu trữ giá rẻ trên MinIO (data lake), nạp vào Snowflake, và biến đổi thành các mart phục vụ báo cáo bằng dbt — toàn bộ được orchestrate bởi Airflow.


## Kiến trúc tổng quan



**Nguyên lý thiết kế:**
- **Immutable raw layer**: dữ liệu CDC gốc trên MinIO không bao giờ bị sửa/xoá — cho phép replay/backfill bất cứ lúc nào.
- **Idempotent ingestion**: mỗi file trên MinIO chỉ được COPY INTO Snowflake đúng 1 lần, kể cả khi Airflow retry nhiều lần.
- **SCD Type 2** cho các dimension thay đổi theo thời gian (`customer`, `account`) bằng dbt snapshot.
- **Append-only fact**: bảng `transaction` không bao giờ update, chỉ insert — giữ đúng bản chất log giao dịch tài chính.

## Công nghệ sử dụng

| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Nguồn OLTP | PostgreSQL 15 (logical replication) | Sinh dữ liệu giao dịch giả lập |
| CDC | Debezium 2.2 (Kafka Connect) | Bắt mọi insert/update/delete từ WAL |
| Message broker | Apache Kafka (KRaft mode) | Truyền tải CDC event |
| Data lake | MinIO (S3-compatible) | Lưu trữ giá rẻ, immutable, làm nguồn replay |
| Data warehouse | Snowflake | Lưu trữ và xử lý dữ liệu phân tích |
| Transform | dbt-snowflake | Biến đổi RAW → staging → snapshot → intermediate → mart |
| Orchestration | Apache Airflow 2.9.3 | Điều phối lịch chạy các bước ETL/ELT |
| Data generator | Python (Faker, psycopg2) | Sinh dữ liệu giao dịch giả lập với tốc độ cấu hình được (TPS) |
| Consumer | Python (kafka-python, boto3) | Đọc Kafka, ghi Parquet lên MinIO |

## Schema nguồn (OLTP)

```sql
CREATE TABLE customer (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customer(id),
    account_type VARCHAR(50),
    balance NUMERIC(18,2),
    currency VARCHAR(50),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE transaction (
    id BIGSERIAL PRIMARY KEY,
    account_id INT REFERENCES account(id),
    txn_type VARCHAR(30),
    amount NUMERIC(18,2),
    related_account_id INT,
    txn_status VARCHAR(20),
    error_code VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
```

## Cấu trúc thư mục

```
.
├── airflow.dockerfile              # Image Airflow có thêm venv riêng cho dbt 
├── docker-compose.yml              
├── requirements.txt
│
├── generator/
│   └── fake_generator.py           # Sinh giao dịch giả lập 
│
├── kafka-debezium/
│   └── kafka_debezium_connector.py # Debezium Postgres connector 
│
├── consumer/
│   └── kafka_to_minio.py           # Consume Kafka, batch, ghi Parquet lên MinIO
│
├── docker/
│   └── dags/
│       ├── minio_to_snowflake.py   # DAG: MinIO (incoming/) → Snowflake RAW → MinIO (processed/)
│       └── scd2_snapnot.py         # DAG: staging → test → snapshot → intermediate → mart
│
└── banking_dbt/                    # dbt project
    ├── dbt_project.yml
    ├── models/
    │   ├── sources.yml
    │   ├── staging/                # Dedup, cast type
    │   │   ├── stg_customer.sql
    │   │   ├── stg_account.sql
    │   │   ├── stg_transaction.sql
    │   │   └── schema.yml          # tests: unique, not_null, accepted_values, relationships
    │   ├── intermediate/
    │   │   ├── dimensions/         # current state, build từ snapshot
    │   │   │   ├── dim_customer.sql   
    │   │   │   └── dim_account.sql
    │   │   └── facts/
    │   │       └── fact_transaction.sql
    │   └── mart/
    │       ├── customer/mart_customer_360.sql
    │       ├── account/mart_account_summary.sql
    │       └── transaction/
    │           ├── mart_daily_transaction.sql
    │           ├── mart_transaction_trend.sql
    │           └── mart_failed_transaction_reason.sql
    ├── snapshots/                  # SCD2, strategy=check, lưu dữ lịch sử
    │   ├── customer_snapshot.yml   
    │   └── account_snapshot.yml
    └── macros/
        └── is_positive.sql         # custom generic test
```

## Luồng dữ liệu chi tiết

### 1. Sinh dữ liệu (`generator/fake_generator.py`)
Sinh khách hàng, tài khoản ban đầu, sau đó chạy vòng lặp liên tục mô phỏng các sự kiện ngân hàng thực tế: `deposit`, `withdraw`, `transfer`, `update_customer_info`, `open_account`, `freeze_account`, `unfreeze_account`, `change_account_type` — có xử lý các trường hợp lỗi thực tế (`INSUFFICIENT_BALANCE`, `ACCOUNT_INACTIVE`). Tốc độ sinh dữ liệu cấu hình qua biến `TPS`.

### 2. Capture thay đổi (Debezium)
`kafka_debezium_connector.py` đăng ký 1 Postgres connector qua Kafka Connect REST API (`POST /connectors`), theo dõi 3 bảng `customer`, `account`, `transaction` bằng logical replication (`pgoutput` plugin). Mỗi thay đổi (insert/update/delete) sinh ra 1 message trên topic tương ứng dạng `banking_server.public.<table>`.

### 3. Consume & lưu trữ (`consumer/kafka_to_minio.py`)
- Đọc message từ 3 Kafka topic, parse Debezium envelope (`op`, `before`, `after`, `ts_ms`).
- Buffer theo batch (flush khi đủ `BATCH_SIZE` hoặc quá `FLUSH_INTERVAL_SEC`).
- Ghi Parquet lên MinIO tại `s3://<bucket>/<table>/incoming/year=.../month=.../day=.../`.
- Chỉ `consumer.commit()` **sau khi mọi buffer đã flush xong** — đảm bảo at-least-once delivery, không mất dữ liệu khi crash giữa chừng.

### 4. Nạp vào Snowflake (`docker/dags/minio_to_snowflake.py`)
DAG Airflow `minio_to_snowflake_banking`:
- **Task 1 – `download_minio`**: liệt kê và tải toàn bộ file trong `<table>/incoming/` về local.
- **Task 2 – `load_snowflake`**: với mỗi file, kiểm tra lại trên MinIO xem key còn tồn tại ở `incoming/` không (`key_exists`) trước khi xử lý — đây là cơ chế chống trùng lặp khi Airflow retry: nếu file đã bị move sang `processed/` ở lần chạy trước, lần retry này sẽ bỏ qua. `PUT` file lên internal stage, `COPY INTO` bảng RAW tương ứng, sau đó move file từ `incoming/` sang `processed/` trên MinIO. Mỗi bảng xử lý độc lập — lỗi ở 1 bảng không chặn các bảng còn lại.

### 5. Transform bằng dbt (`docker/dags/scd2_snapnot.py`)
DAG `SCD2_snapshots` chạy tuần tự:
```
dbt run --select staging → dbt test → dbt snapshot → dbt run --select intermediate → dbt run --select mart
```

## dbt Data Warehouse Layers

| Layer | Materialization | Vai trò |
|---|---|---|
| **RAW** | External stage / table trong Snowflake | Dữ liệu Debezium thô, chưa xử lý |
| **Staging** | `view` | Parse JSON (`VARIANT`), dedup theo `id` (giữ 1 bản/record), cast kiểu dữ liệu |
| **Snapshot** | `table` (dbt snapshot, strategy `check`) | Lưu lịch sử đầy đủ (SCD Type 2) của `customer`, `account` |
| **Intermediate (dimensions)** | `table` | Lọc từ snapshot chỉ lấy bản ghi hiện tại (`dbt_valid_to is null`) |
| **Intermediate (facts)** | `table`, `incremental` | `fact_transaction`: join transaction với dim_account, incremental theo `transaction_time` |
| **Mart** | `table` | Các bảng phục vụ báo cáo trực tiếp: `mart_customer_360`, `mart_account_summary`, `mart_daily_transaction`, `mart_transaction_trend`, `mart_failed_transaction_reason` |

### Các mart báo cáo hiện có

- **`mart_customer_360`**: hồ sơ khách hàng tổng hợp — số tài khoản, tổng số dư, tổng/tỷ lệ giao dịch thất bại, giao dịch đầu/cuối.
- **`mart_account_summary`**: tổng hợp theo loại tài khoản — số lượng, trạng thái active/frozen, tổng và trung bình số dư.
- **`mart_daily_transaction`**: khối lượng giao dịch theo ngày, tách theo loại giao dịch (deposit/transfer/withdraw), tỷ lệ thất bại.
- **`mart_transaction_trend`**: phân bố giao dịch theo giờ trong ngày.
- **`mart_failed_transaction_reason`**: phân tích nguyên nhân giao dịch thất bại (`error_code`) theo ngày và loại giao dịch, kèm tỷ lệ phần trăm.

## Cài đặt & chạy dự án

### Yêu cầu
- Docker & Docker Compose
- Tài khoản Snowflake (database, warehouse, user có quyền COPY INTO)
- File `.env` khai báo các biến môi trường (xem bảng bên dưới)

### Biến môi trường cần thiết (`.env`)

```env
# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=banking
POSTGRES_USER=postgres
POSTGRES_PASSWORD=changeme

# Airflow metadata DB
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=banking-cdc-lake
MINIO_LOCAL_DIR=/tmp/minio_downloads

# Kafka
KAFKA_BOOTSTRAP=localhost:29092
KAFKA_GROUP=banking-consumer-group

# Snowflake
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DB=banking
SNOWFLAKE_SCHEMA=raw
```

### Các bước chạy

```bash
# 1. Khởi động toàn bộ hạ tầng
docker compose up -d 

# 2. Đăng ký Debezium Postgres connector
cd kafka-debezium
python kafka_debezium_connector.py

# 3. Sinh dữ liệu giả lập (chạy ở máy host hoặc container riêng)
cd generator
python fake_generator.py
# nhập số lượng khách hàng ban đầu khi được hỏi

# 4. Chạy consumer để đẩy dữ liệu từ Kafka lên MinIO
cd consumer
python kafka_to_minio.py

# 5. Trigger DAG nạp dữ liệu vào Snowflake 
#    DAG: minio_to_snowflake_banking

# 6. Trigger DAG transform dbt
#    DAG: SCD2_snapshots
```






