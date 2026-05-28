select
*,
country.is_eu,
country.region
from {{ ref('base_ecommerce_customers')}} as cust
    left join {{ref('countries')}} as country
        on cust.country = country.country