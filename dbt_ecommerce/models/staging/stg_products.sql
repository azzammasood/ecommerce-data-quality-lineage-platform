with source as (
    select * from {{ source('ecommerce_raw', 'products') }}
),
renamed as (
    select
        product_id,
        name as product_name,
        category,
        price
    from source
)
select * from renamed
