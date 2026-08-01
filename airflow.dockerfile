FROM apache/airflow:2.9.3
USER airflow

# Tạo môi trường ảo tên là 'dbt_venv' nằm trong thư mục /opt/airflow/
RUN python -m venv /opt/airflow/dbt_venv

# Sử dụng pip CỦA MÔI TRƯỜNG ẢO để cài đặt dbt
RUN /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-snowflake