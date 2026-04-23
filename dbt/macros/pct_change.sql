-- Macro: calculate percent change between two columns
-- Usage: {{ pct_change('new_value', 'old_value') }}
-- Returns: (new - old) / old

{% macro pct_change(new_col, old_col) %}
    div0({{ new_col }} - {{ old_col }}, {{ old_col }})
{% endmacro %}
