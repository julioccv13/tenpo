{% macro proper_name() %}

CREATE OR REPLACE FUNCTION {{target.schema}}.proper_name(str STRING) AS (( 
  SELECT STRING_AGG(CONCAT(UPPER(SUBSTR(w,1,1)), LOWER(SUBSTR(w,2))), ' ' ORDER BY pos) 
  FROM UNNEST(SPLIT(str, ' ')) w WITH OFFSET pos
));


{% endmacro %}
