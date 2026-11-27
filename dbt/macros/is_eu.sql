{% macro is_eu(column_name) %}
    {{column_name}} IN ('FR', 'DE')
{% endmacro %}
