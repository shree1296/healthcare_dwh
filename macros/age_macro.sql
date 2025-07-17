{% macro calculate_age(dob_column) %}
    datediff(year, {{ dob_column }}, current_date())    
{% endmacro %}