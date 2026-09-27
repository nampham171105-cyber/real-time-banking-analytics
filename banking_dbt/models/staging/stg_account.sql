{{ config(materialized = 'view') }}

with ranked as (
    select
        v:id::string            as account_id,
        v:customer_id::string   as customer_id,
        v:account_type::string  as account_type,
        v:balance::number(18,2) as balance,
        v:currency::string      as currency,
        v:status::string        as status,
        v:created_at::timestamp as created_at,
        _loaded_at              as load_timestamp,
        _source_file            as source_file,
        row_number() over (
            partition by v:id::string
            order by v:_ts_ms::number desc
        ) as rn
    from
        {{ source('raw','account') }}
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    status,
    created_at,
    load_timestamp,
    source_file
from
    ranked
where
    rn = 1