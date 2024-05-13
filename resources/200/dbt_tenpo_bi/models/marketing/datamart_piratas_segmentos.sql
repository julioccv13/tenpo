{{ 
    config(
        materialized='table'
        ,tags=["hourly", "bi"]
        ,project=env_var('DBT_PROJECT19', 'tenpo-datalake-sandbox')
        ,schema='segments'
        ,alias='segmentos_rfmp_bancarization'
    )
}}

WITH RFMP AS (
  SELECT DISTINCT
  ID,
  ULTIMO_SEGMENTO_RFMP_60
  FROM (
    SELECT DISTINCT
      user id,
      email,
      LAST_VALUE(Fecha_Fin_Analisis_DT) OVER (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ultima_fecha_rfmp_60,
      LAST_VALUE(segment_ult60d) OVER (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ultimo_segmento_rfmp_60
    FROM {{ source('tablones_analisis', 'tablon_rfmp_v2') }}   --`tenpo-bi.tablones_analisis.tablon_rfmp_v2`
    )
),

BANCARIZATION AS (
SELECT DISTINCT
USER ID,
CARACTERIZACION
 FROM {{ ref('bank_characterization') }} --`tenpo-bi-prod.bancarization.bank_characterization` 
),

USERS AS (
  SELECT DISTINCT 
ID,
EMAIL,
RUT,
PHONE,
STATE,
ob_completed_at  FROM {{ source('tenpo_users', 'users') }} --`tenpo-airflow-prod.users.users` 
)

SELECT 
*
FROM USERS
LEFT JOIN BANCARIZATION USING(ID)
LEFT JOIN RFMP USING(ID)