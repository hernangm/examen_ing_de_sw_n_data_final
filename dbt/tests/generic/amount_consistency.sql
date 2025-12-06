{% test amount_consistency(model, column_greater, column_less) %}
select
    *
from {{ model }}
where {{ column_greater }} < {{ column_less }}
{% endtest %}
