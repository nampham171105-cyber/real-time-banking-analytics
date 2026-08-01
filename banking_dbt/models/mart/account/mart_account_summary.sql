{{ config(materialized = 'table') }}

with accounts as (
    select * from {{ ref('dim_account') }}
),
account_summary as (
    select 
        account_type,
        count(*) as account_count,
        count(case when status = 'ACTIVE' then 1 end) as active_accounts,
        coalesce(sum(balance), 0) as total_balance,
        coalesce(avg(balance), 0) as avg_balance
    from accounts
    group by account_type
)

select * from account_summary