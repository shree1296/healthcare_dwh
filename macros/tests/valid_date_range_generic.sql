{% test valid_date_range(model, column_name) %}
    select * from {{ model }}
    where {{ column_name }} < '2000-01-01' or {{ column_name }} > current_date()
{% endtest%}