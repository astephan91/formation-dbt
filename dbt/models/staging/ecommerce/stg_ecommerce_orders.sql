{{ config(
    materialized="incremental",
    unique_key="order_id"
) }}

SELECT

order_id,
customer_id,
order_date,
'test' as newcol
FROM {{ ref('base_ecommerce_orders')}}

WHERE TRUE
{% if is_incremental() %}
AND order_date > (select coalesce(max(order_date),'1900-01-01') from {{ this }} )
{% endif %}