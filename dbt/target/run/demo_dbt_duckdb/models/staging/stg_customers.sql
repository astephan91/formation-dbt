
  
  create view warehouse.analytics_staging.stg_customers__dbt_tmp as (
    with source as (
    select * from "warehouse"."raw"."customers"
),

renamed as (
    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(email) as email,
        upper(country) as country,
        cast(signup_date as date) as signup_date,
        concat(trim(first_name), ' ', trim(last_name)) as full_name
    from source
)

select * from renamed
  );
