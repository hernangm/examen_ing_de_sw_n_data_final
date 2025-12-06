{% test is_valid_status_transition(model, status_column, amount_column) %}
-- This test fails if a transaction is 'completed' but has a zero or negative amount.
select
    *
from {{ model }}
where
    {{ status_column }} = 'completed' and {{ amount_column }} <= 0
{% endtest %}
