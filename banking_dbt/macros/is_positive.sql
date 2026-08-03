{% test is_positive(model, column_name, allow_zero=False) %}

    select *
    from {{ model }}
    where 
        {% if allow_zero %}
            {{ column_name }} < 0   
        {% else %}
            {{ column_name }} <= 0  
        {% endif %}

{% endtest %}