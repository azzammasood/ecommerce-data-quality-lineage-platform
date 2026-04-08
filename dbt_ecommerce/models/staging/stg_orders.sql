select
    order_id,
    user_id,
    status        as order_status,
    order_date    as order_created_at
from {{ source('ecommerce_raw', 'orders') }}
