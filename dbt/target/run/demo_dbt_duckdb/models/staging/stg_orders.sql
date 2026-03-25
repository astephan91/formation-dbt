
  
  create view warehouse.analytics_staging.stg_orders__dbt_tmp as (
    with source as (
    select * from "warehouse"."raw"."orders"
),

typed as (
    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        cast(amount as double) as amount,
        lower(status) as status
    from source
)

select * from typed
  );
