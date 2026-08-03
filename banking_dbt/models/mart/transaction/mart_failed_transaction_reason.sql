{{ config(materialized='table') }}

with failed_transactions as (
     select
        cast(transaction_time as date) as txn_date,
        transaction_type,
        error_code,
        amount
    from {{ ref('fact_transaction') }}
    where txn_status = 'FAILED'

),

daily_failed_total as (
    select
        txn_date,
        count(*) as total_failed
    from failed_transactions
    group by txn_date

),

failed_reason as (
    select
        txn_date,
        transaction_type,
        error_code,
        count(*) as failed_count,
        sum(amount) as failed_amount
    from failed_transactions
    group by
        txn_date,
        transaction_type,
        error_code
)

select
    fr.txn_date,
    fr.transaction_type,
    fr.error_code,
    fr.failed_count,
    fr.failed_amount,
    round(fr.failed_count::numeric / nullif(d.total_failed,0),4) as percentage
from failed_reason fr
join daily_failed_total d
    on fr.txn_date = d.txn_date
order by
    fr.txn_date,
    fr.failed_count desc