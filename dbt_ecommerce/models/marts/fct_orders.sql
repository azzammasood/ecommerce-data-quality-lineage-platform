with orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
order_totals as (
    select
        order_id,
        sum(quantity * price_at_purchase) as total_amount,
        sum(quantity) as total_items
    from order_items
    group by 1
)

select
    o.order_id,
    o.user_id,
    o.order_status,
    coalesce(t.total_amount, 0) as total_amount,
    coalesce(t.total_items, 0) as total_items
from orders o
left join order_totals t on o.order_id = t.order_id
