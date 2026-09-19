{{ config(materialized = 'table') }}

with lastest_customer as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        dbt_valid_from as effective_from,
        dbt_valid_to as effective_to
    from
        {{ ref("customer_snapshot")}}
    where dbt_valid_to is null
      and is_deleted = false
)

select * from lastest_customer