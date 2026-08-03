{{ config(materialized = 'table')}}

with transactions as (
    select * from {{ ref('fact_transaction') }}
),

transaction_trend as (
    select 
        extract(hour from transaction_time) as hour_of_day,
        count(*) as total_transaction,
        coalesce(sum(amount), 0) as total_amount,
        count(case when transaction_type = 'DEPOSIT' then 1 end) as deposit_count,
        count(case when transaction_type = 'TRANSFER' then 1 end) as transfer_count,
        count(case when transaction_type = 'WITHDRAW' then 1 end) as withdraw_count
    from transactions
    group by extract(hour from transaction_time)
)

select * from transaction_trend
order by hour_of_day asc