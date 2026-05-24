with source as (
    select * from {{ source('raw', 'order_items') }}
),

renamed as (
    select
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price
    from source
)

select * from renamed
