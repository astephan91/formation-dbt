SELECT
order_id,
customer_id,
order_date,
status
FROM {{ ref('base_ecommerce_orders')}}