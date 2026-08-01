{{ config(materialized = 'table' ) }}

with transactions as (
    select 
        cast(transaction_time as date) as txn_date,
        transaction_type,
        amount
    from {{ ref('fact_transaction') }}
),

daily_summary as (
    select 
        txn_date,
        count(*) as total_transaction,
        sum(amount) as total_amount,
        count(case when transaction_type = 'DEPOSIT' then 1 end) as deposit_count,
        count(case when transaction_type = 'TRANSFER' then 1 end) as transfer_count,
        count(case when transaction_type = 'WITHDRAW' then 1 end) as withdraw_count,
        sum(case when transaction_type = 'DEPOSIT' then amount else 0 end) as deposit_amount,
        sum(case when transaction_type = 'TRANSFER' then amount else 0 end) as transfer_amount,
        sum(case when transaction_type = 'WITHDRAW' then amount else 0 end) as withdraw_amount

    from transactions
    group by txn_date
)

select * 
from daily_summary