{{ config(materialized = 'table')}}

with transactions as (
    select * from {{ ref('fact_transaction') }}
),

transaction_trend as (
    select 
        extract(hour from transaction_time) as hour_of_day,
        count(transaction_id) as total_transaction,
        coalesce(sum(amount), 0) as total_amount
    from transactions
    group by extract(hour from transaction_time)
)

select * from transaction_trend
order by hour_of_day asc