with orders as (
    select * from {{ ref('fct_orders') }}
),

paid_orders as (
    select *
    from orders
    where status in ('paid', 'shipped')
),

agg as (
    select
        customer_id,
        count(*) as nb_orders,
        sum(amount) as total_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from paid_orders
    group by 1
)

select
    c.customer_id,
    c.full_name,
    c.country,
    coalesce(a.nb_orders, 0) as nb_orders,
    coalesce(a.total_revenue, 0) as total_revenue,
    a.first_order_date,
    a.last_order_date
from {{ ref('dim_customers') }} c
left join agg a using (customer_id)
