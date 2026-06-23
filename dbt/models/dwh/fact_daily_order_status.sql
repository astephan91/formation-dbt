{% set order_status_query %}
select distinct status from {{ref('stg_ecommerce_orders')}}
{% endset %}

{% set results = run_query(order_status_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

select
order_date,
{% for status in results_list %}
count(case when status = '{{ status }}' then order_id end) as {{ status }}_nb_orders,
{% endfor %}
count(order_id) as total_orders
from {{ref('stg_ecommerce_orders')}}
group by 1