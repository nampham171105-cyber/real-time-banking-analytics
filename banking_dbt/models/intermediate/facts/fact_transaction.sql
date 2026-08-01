{{ config(materialized = 'incremental', unique_key = 'transaction_id') }}

select
    t.transaction_id,
    t.transaction_uuid,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.txn_status,
    t.transaction_type,
    t.transaction_time,
    CURRENT_TIMESTAMP as load_timestamp
from 
    {{ ref('stg_transaction') }} t
left join
    {{ ref('dim_account') }} a
on t.account_id = a.account_id

{% if is_incremental() %}

where t.transaction_time > (select max(transaction_time) from {{this}})

{% endif %}