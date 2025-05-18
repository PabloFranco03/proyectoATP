{% macro filtrado_copa_davis(nivel_col) %}
  {{ nivel_col }} NOT IN('D','F')
{% endmacro %}