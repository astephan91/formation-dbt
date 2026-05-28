select
*
from {{ source('ecommerce', 'customers') }}