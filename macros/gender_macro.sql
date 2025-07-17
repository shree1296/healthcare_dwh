{% macro standardize_gender(column_name) %}
    case lower({{ column_name }})
        when 'm' then 'M'
        when 'male' then 'M'
        when 'f' then 'F'
        when 'female' then 'F'
        else 'Others'
    end
{% endmacro %}
