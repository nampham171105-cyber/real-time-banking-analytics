{{ config(materialized = 'view' )}}

with ranked as (
    select
        v:id::string            as customer_id,
        v:first_name::string    as first_name,
        v:last_name::string     as last_name,
        v:email::string         as email,
        v:created_at::timestamp as created_at,
        v:is_deleted::boolean   as is_deleted,
        _loaded_at              as load_timestamp,
        _source_file            as source_file,
        row_number() over (
            partition by v:id::string
            order by v:_ts_ms::number desc
        ) as rn
    from
        {{ source('raw', 'customer') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    is_deleted,
    load_timestamp,
    source_file
from
    ranked
where
    rn = 1