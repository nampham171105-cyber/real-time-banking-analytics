{{ config(materialized = 'incremental', unique_key = 'transaction_id', incremental_strategy = 'merge') }}

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.txn_status,
    t.error_code,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP as load_timestamp
from
    {{ ref('stg_transaction') }} t
left join
    {{ ref('dim_account') }} a
on t.account_id = a.account_id

{% if is_incremental() %}
-- Lookback 2 giờ để không bỏ sót dữ liệu đến trễ do Kafka lag / clock skew.
-- Kết hợp merge strategy (unique_key) nên record đến trễ được merge đúng,
-- không tạo duplicate dù nằm trong vùng lookback đã xử lý trước đó.
where t.transaction_time > (select dateadd(hour, -2, max(transaction_time)) from {{this}})
{% endif %}