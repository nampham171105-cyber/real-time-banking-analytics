# Banking Real-Time Data Pipeline (CDC → Kafka → MinIO → Snowflake → dbt)

Một data pipeline end-to-end mô phỏng hệ thống ngân hàng, capture thay đổi dữ liệu theo thời gian thực từ Postgres bằng Debezium (CDC), đẩy qua Kafka, lưu trữ giá rẻ trên MinIO (data lake), nạp vào Snowflake, và biến đổi thành các mart phục vụ báo cáo bằng dbt — toàn bộ được orchestrate bởi Airflow.

## Kiến trúc tổng quan

<img width="1255" height="617" alt="image" src="https://github.com/user-attachments/assets/25643118-89f3-483c-afd5-d7dd7e078f07" />

**Nguyên lý thiết kế:**
- **Immutable raw layer**: dữ liệu CDC gốc trên MinIO không bao giờ bị sửa/xoá — cho phép replay/backfill bất cứ lúc nào.
- **Idempotent ingestion**: mỗi file trên MinIO chỉ được COPY INTO Snowflake đúng 1 lần, kể cả khi Airflow retry nhiều lần, và `max_active_runs=1` đảm bảo không có 2 lần chạy DAG chồng lên nhau đọc cùng file.
- **SCD Type 2** cho các dimension thay đổi theo thời gian (`customer`, `account`) bằng dbt snapshot.
- **Append-only fact**: bảng `transaction` không bao giờ update, chỉ insert — giữ đúng bản chất log giao dịch tài chính.
- **Soft-delete**: hệ thống không bao giờ `DELETE` vật lý customer/account — "xoá" khách hàng nghĩa là đánh cờ `is_deleted = true` và đóng toàn bộ tài khoản liên quan (`status = 'CLOSED'`). 
- **Transactional integrity ở tầng sinh dữ liệu**: các thao tác nhiều bước (chuyển tiền, đóng khách hàng) dùng `SAVEPOINT` để đảm bảo không có trạng thái "nửa vời" khi lỗi giữa chừng.
- **Event-driven orchestration**: DAG transform (dbt) được trigger tự động qua Airflow Dataset ngay khi có dữ liệu mới, thay vì chạy theo lịch cố định độc lập không liên quan tới DAG ingest.

## Công nghệ sử dụng

| Thành phần | Công nghệ | Vai trò |
|---|---|---|
| Nguồn OLTP | PostgreSQL 15 (logical replication) | Sinh dữ liệu giao dịch giả lập |
| CDC | Debezium 2.2 (Kafka Connect) | Bắt insert/update từ WAL (không có DELETE vật lý) |
| Message broker | Apache Kafka (KRaft mode) | Truyền tải CDC event |
| Data lake | MinIO (S3-compatible) | Lưu trữ giá rẻ, immutable, làm nguồn replay, kèm khu vực DLQ cho message lỗi |
| Data warehouse | Snowflake | Lưu trữ và xử lý dữ liệu phân tích |
| Transform | dbt-snowflake | Biến đổi RAW → staging → snapshot → intermediate → mart |
| Orchestration | Apache Airflow 2.9.3 (Datasets) | Điều phối lịch chạy và trigger phụ thuộc giữa các DAG |
| Data generator | Python (Faker, psycopg2) | Sinh dữ liệu giao dịch giả lập với tốc độ cấu hình được (TPS) |
| Consumer | Python (kafka-python, boto3) | Đọc Kafka, ghi Parquet lên MinIO, cách ly poison message vào DLQ |

## Schema nguồn (OLTP)

```sql
CREATE TABLE customer (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    is_deleted BOOLEAN DEFAULT false,   -- soft-delete: "xoá" = update cờ này
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE account (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customer(id),
    account_type VARCHAR(50),
    balance NUMERIC(18,2),
    currency VARCHAR(50),
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE | FROZEN | CLOSED
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

3 bảng này **được tự động tạo** khi Postgres container khởi tạo lần đầu (xem mục Cài đặt bên dưới) — không cần chạy tay.

## Cấu trúc thư mục

```
.
├── airflow.dockerfile
├── docker-compose.yml              # mọi service đều có healthcheck + depends_on condition
├── requirements.txt
│
├── docker/ 
│   ├── snowflake/
│   │   └── init_snowflake.sql   # khởi tạo ban đầu trong snowflake
│   ├── postgres/
│   │   └── init/
│   │       └── init_db.sql      # tự động chạy khi Postgres khởi tạo lần đầu 
│   └── dags/
│       ├── minio_to_snowflake.py   # DAG: MinIO (incoming/) → Snowflake RAW → MinIO (processed/), phát Dataset event
│       └── scd2_snapnot.py         # DAG: staging → test → snapshot → intermediate → mart, trigger qua Dataset
|    
│
├── generator/
│   └── fake_generator.py           # Sinh giao dịch giả lập, transfer()/close_customer() dùng SAVEPOINT
│
├── kafka-debezium/
│   └── kafka_debezium_connector.py # Debezium Postgres connector
│
├── consumer/
│   └── kafka_to_minio.py           # Consume Kafka, batch, ghi Parquet lên MinIO, DLQ cho poison message
│
│
└── banking_dbt/                    # dbt project
    ├── dbt_project.yml
    ├── models/
    │   ├── sources.yml
    │   ├── staging/
    │   │   ├── stg_customer.sql    # đọc is_deleted trực tiếp từ cột nguồn
    │   │   ├── stg_account.sql
    │   │   ├── stg_transaction.sql
    │   │   └── schema.yml
    │   ├── intermediate/
    │   │   ├── schema.yml
    │   │   ├── dimensions/
    │   │   │   ├── dim_customer.sql   # lọc is_deleted = false
    │   │   │   └── dim_account.sql    # KHÔNG lọc CLOSED — mart cần đếm được account đã đóng
    │   │   └── facts/
    │   │       └── fact_transaction.sql  # incremental + merge, lookback 2h
    │   └── mart/
    │       ├── customer/mart_customer_360.sql
    │       ├── account/mart_account_summary.sql
    │       └── transaction/
    │           ├── mart_daily_transaction.sql
    │           ├── mart_transaction_trend.sql
    │           └── mart_failed_transaction_reason.sql
    ├── snapshots/
    │   ├── customer_snapshot.yml   # check_cols bao gồm is_deleted
    │   └── account_snapshot.yml    # check_cols: status (đã bao gồm CLOSED)
    └── macros/
        └── is_positive.sql
```

## Luồng dữ liệu chi tiết

### 1. Sinh dữ liệu (`generator/fake_generator.py`)
Sinh khách hàng, tài khoản ban đầu, sau đó chạy vòng lặp liên tục mô phỏng các sự kiện ngân hàng thực tế: `deposit`, `withdraw`, `transfer`, `update_customer_info`, `create_customer`, `open_account`, `freeze_account`, `unfreeze_account`, `change_account_type`, `close_customer`. Tốc độ sinh dữ liệu cấu hình qua biến `TPS`.

- `transfer()` và `close_customer()` dùng `SAVEPOINT`: nếu lỗi giữa chừng, chỉ giao dịch/thao tác đó bị rollback — các event khác trong cùng batch commit vẫn an toàn.
- `close_customer()` **không** `DELETE` — chỉ đánh cờ `is_deleted = true` cho customer và `status = 'CLOSED'` cho toàn bộ account của họ.

### 2. Capture thay đổi (Debezium)
`kafka_debezium_connector.py` đăng ký 1 Postgres connector qua Kafka Connect REST API, theo dõi 3 bảng bằng logical replication (`pgoutput`).

### 3. Consume & lưu trữ (`consumer/kafka_to_minio.py`)
- Đọc message từ 3 Kafka topic, parse Debezium envelope, buffer theo batch (`BATCH_SIZE=5000` hoặc `FLUSH_INTERVAL_SEC=120`).
- Ghi Parquet lên MinIO tại `s3://<bucket>/<table>/incoming/year=.../month=.../day=.../`.
- Chỉ `consumer.commit()` sau khi mọi buffer đã flush xong — đảm bảo at-least-once delivery.
- **Dead Letter Queue**: message không parse được ghi vào `s3://<bucket>/_dlq/<table>/...` kèm lý do lỗi, không làm treo consumer.
- **Backoff tăng dần** khi Kafka/MinIO gián đoạn (tối đa 60s), tránh spam retry khi outage kéo dài.

### 4. Nạp vào Snowflake (`docker/dags/minio_to_snowflake.py`)
DAG `minio_to_snowflake_banking` (lịch chạy: mỗi 10 phút, `max_active_runs=1`):
- Liệt kê + tải file `<table>/incoming/`, kiểm tra `key_exists` trước khi xử lý để chống trùng khi Airflow retry.
- `PUT` + `COPY INTO` bảng RAW, move file sang `processed/` sau khi thành công. Mỗi bảng xử lý độc lập.
- Nếu không có file mới, task **skip** thay vì "success rỗng" — tránh trigger DAG transform vô ích.
- Khi load thành công và có dữ liệu mới, phát ra **Airflow Dataset event** (`snowflake://banking/raw/loaded`).

### 5. Transform bằng dbt (`docker/dags/scd2_snapnot.py`)
DAG `SCD2_snapshots` **không chạy theo lịch cố định**, được trigger tự động ngay khi có Dataset event từ bước 4:
```
dbt run --select staging → dbt test → dbt snapshot → dbt run --select intermediate → dbt run --select mart
```
> **Lưu ý:** DAG mới trong Airflow mặc định bị **pause** khi lần đầu deploy. Nhớ **unpause `SCD2_snapshots` trên UI** sau lần đầu chạy `docker compose up`, nếu không Dataset event sẽ không tạo được DagRun dù đã được phát ra đúng.

## dbt Data Warehouse Layers

| Layer | Materialization | Vai trò |
|---|---|---|
| **RAW** | Table `VARIANT` trong Snowflake | Dữ liệu Debezium thô, chưa xử lý |
| **Staging** | `view` | Parse JSON, dedup theo `id` (giữ bản mới nhất), cast kiểu dữ liệu |
| **Snapshot** | `table` (dbt snapshot, strategy `check`) | Lưu lịch sử đầy đủ (SCD Type 2) của `customer` (bao gồm `is_deleted`), `account` (bao gồm `status`) |
| **Intermediate (dimensions)** | `table` | `dim_customer` lọc `is_deleted = false`; `dim_account` giữ nguyên mọi trạng thái kể cả `CLOSED` |
| **Intermediate (facts)** | `table`, `incremental` (merge) | `fact_transaction`: incremental theo `transaction_time`, lookback 2 giờ cho dữ liệu đến trễ |
| **Mart** | `table` | `mart_customer_360`, `mart_account_summary`, `mart_daily_transaction`, `mart_transaction_trend`, `mart_failed_transaction_reason` |

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
POSTGRES_PASSWORD=postgres

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
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DB=banking
SNOWFLAKE_SCHEMA=raw
```

### Các bước chạy

```bash
# 1. Setup Snowflake — chạy TAY 1 lần trên Snowflake Web UI.
copy file init_snowflake lên snowflake, bôi đen rồi chạy

# 2. Khởi động hạ tầng — Postgres tự tạo sẵn 3 bảng customer/account/transaction
docker compose up -d

# 3. Đăng ký Debezium Postgres connector
cd kafka-debezium
python kafka_debezium_connector.py

# 4. Sinh dữ liệu giả lập (chạy ở máy host hoặc container riêng)
cd generator
python fake_generator.py
# nhập số lượng khách hàng ban đầu khi được hỏi

# 5. Chạy consumer để đẩy dữ liệu từ Kafka lên MinIO
cd consumer
python kafka_to_minio.py

# 6. Trên Airflow UI:
#    - Unpause DAG minio_to_snowflake_banking (chạy mỗi 10 phút)
#    - Unpause DAG SCD2_snapshots (sẽ TỰ ĐỘNG chạy sau khi có dữ liệu mới, không cần trigger tay)

```

## Known limitations / Hướng phát triển tiếp theo

Dự án ưu tiên xử lý theo thứ tự: **tính đúng đắn dữ liệu → độ tin cậy pipeline**. Các hạng mục sau chưa hoàn thiện, không ảnh hưởng tính đúng đắn dữ liệu hiện tại:

- **Alerting**: chưa có `on_failure_callback` gửi cảnh báo (Slack/email) khi DAG hoặc dbt test fail.
- **Snowpipe auto-ingest**: hiện dùng Airflow polling MinIO mỗi 10 phút; có thể nâng cấp lên push-based ingestion để giảm độ trễ hơn nữa.
- **CI/CD**: chưa có pipeline tự động chạy `dbt run`/`dbt test` trên pull request.
