{% test is_valid_email(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} not like '%@%.%'

{% endtest %}
