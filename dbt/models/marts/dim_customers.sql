select
    customer_id,
    full_name,
    email,
    country,
    signup_date
from {{ ref('stg_customers') }}
