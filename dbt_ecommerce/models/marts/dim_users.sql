with users as (
    select * from {{ ref('stg_users') }}
)
select
    user_id,
    email,
    -- Extract domain from email if needed
    split_part(email, '@', 2) as email_domain
from users
