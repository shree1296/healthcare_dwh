{% macro create_temp_table_ctas(table_name, query) %}
    {% set sql %}
        create or replace temporary table {{ table_name }} as
        {{ query }}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
