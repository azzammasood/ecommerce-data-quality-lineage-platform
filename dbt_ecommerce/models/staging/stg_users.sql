select
    user_id,
    first_name,
    last_name,
    email,
    created_at as user_created_at
from {{ source('ecommerce_raw', 'users') }}
