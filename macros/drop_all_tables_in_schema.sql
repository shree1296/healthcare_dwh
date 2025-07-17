{% macro drop_all_tables_in_schema(database, schema) %}

    {% set get_tables_query %}
        select table_name from {{ database }}.information_schema.tables
        where table_schema = '{{ schema }}'
        and table_type = 'BASE TABLE' --only physical tables not views
    {% endset %}

    {# Step 2: Run the query and store the results into a variable using dbt's run_query #}
    {% set results = run_query(get_tables_query) %}

    {# Step 3: Check that the macro is running in an execution context AND results exist #}

{% if execute and results %}
    {# extract the list of tables from results #}
    {% set table_names = results.columns[0].values() %}

    {# loop through each table name and generate a drop table statement #}
    {% for table_name in table_names %}
        {% set drop_statement %}
            DROP TABLE IF EXISTS {{ database }}.{{ schema }}.{{ table_name }}
        {% endset %}

        {# execute the drop table statement using run_query again #}
        {% do run_query(drop_statement) %}
    {% endfor %}
{% endif %}
{% endmacro %}