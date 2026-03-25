select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.status
from warehouse.analytics_staging.stg_orders o