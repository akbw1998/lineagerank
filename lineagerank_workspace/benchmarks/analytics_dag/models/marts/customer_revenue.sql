with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
paid_orders as (
    select
        order_id,
        sum(payment_amount) as paid_amount
    from payments
    where payment_status = 'paid'
    group by 1
)
select
    c.customer_id,
    c.email,
    count(distinct o.order_id) as order_count,
    round(sum(coalesce(p.paid_amount, 0)), 2) as gross_revenue,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from customers c
left join orders o on c.customer_id = o.customer_id
left join paid_orders p on o.order_id = p.order_id
group by 1, 2
