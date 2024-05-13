
{% macro Multiplier() %}

CREATE OR REPLACE FUNCTION {{target.schema}}.Multiplier(d DATE) AS (( 
  (32 - EXTRACT(DAY FROM DATE_ADD(DATE_TRUNC(d, MONTH), INTERVAL 31 DAY)))/EXTRACT(DAY from d)
));

{% endmacro %}
