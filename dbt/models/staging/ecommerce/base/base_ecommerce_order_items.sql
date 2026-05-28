select
*
from {{ source('ecommerce', 'order_items') }}