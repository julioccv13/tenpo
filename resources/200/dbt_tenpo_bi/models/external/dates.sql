{{ 
  config(
    materialized='table',
  ) 
}}

WITH dates AS (SELECT date
      FROM UNNEST(
          GENERATE_DATE_ARRAY(DATE_TRUNC('2020-01-01', YEAR), date(CURRENT_DATE()), INTERVAL 1 DAY)) AS date
      ORDER BY date desc)
SELECT * FROM dates


{% if is_incremental() %}

  where date > (select max(date) from {{ this }})

{% endif %}