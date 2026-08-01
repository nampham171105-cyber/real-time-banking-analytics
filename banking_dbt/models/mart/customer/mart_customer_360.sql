{{ config(materialized = 'table') }}

with customers as (
    select  
        customer_id,
        first_name,
        last_name,
        email
    from {{ ref('dim_customer') }}
),

accounts as (
    select 
        customer_id,
        account_id,
        balance
    from {{ ref('dim_account') }}
),

transactions as (
    select 
        customer_id,
        account_id,
        transaction_id,
        amount,
        transaction_time
    from {{ ref('fact_transaction') }}
),

customer_accounts as (
    select 
        customer_id,
        count(account_id) as account_count,
        sum(balance) as total_balance,
    from accounts
    group by customer_id
),

customer_transactions as (
    select
        a.customer_id,
        count(t.transaction_id) as total_transaction,
        sum(t.amount) as total_transaction_amount,
        min(t.transaction_time) as first_transaction,
        max(t.transaction_time) as last_transaction
    from accounts a
    left join transactions t
        on a.account_id = t.account_id
    group by a.customer_id
),

customer_360 as (
    select
        c.customer_id,
        concat(c.first_name, ' ', c.last_name) as full_name,
        c.email,
        ca.account_count,
        coalesce(ca.total_balance, 0) as total_balance,
        coalesce(ct.total_transaction, 0) as total_transaction,
        coalesce(ct.total_transaction_amount, 0) as total_transaction_amount,
        ct.first_transaction,
        ct.last_transaction
    from customers c
    left join customer_accounts ca
        on c.customer_id = ca.customer_id
    left join customer_transactions ct
        on c.customer_id = ct.customer_id
)

select * from customer_360
