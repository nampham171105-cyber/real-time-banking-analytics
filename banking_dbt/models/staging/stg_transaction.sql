{{ config(materialized = 'view') }}

select
    v:id::string                 as transaction_id,
    v:account_id::string         as account_id,
    v:amount::number(18,2)       as amount,
    v:txn_type::string           as transaction_type,
    v:related_account_id::string as related_account_id,
    v:txn_status::string         as txn_status,
    v:error_code::string         as error_code,
    v:created_at::timestamp      as transaction_time,
    CURRENT_TIMESTAMP            as load_timestamp
from 
    {{ source('raw', 'transaction') }}