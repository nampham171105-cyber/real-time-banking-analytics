{{ config(materialized = 'table') }}

with lastest_account as (
    select
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        status,
        created_at,
        dbt_valid_from as effective_from,
        dbt_valid_to as effective_to,
        case when dbt_valid_to is null then true else false end as is_current
    from
        {{ ref("account_snapshot")}}
)

select * from lastest_account