select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.status
from {{ ref('stg_orders') }} o
