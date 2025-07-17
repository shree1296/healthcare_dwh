{% test valid_gender(model, column_name) %}
    select * from {{ model }} where {{ column_name }} not in ('Male', 'Female')
{% endtest %}