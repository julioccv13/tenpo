{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

WITH 

  last_rfmp as (
    SELECT DISTINCT
      user id,
      LAST_VALUE(segment_ult60d) OVER (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ultimo_segmento_rfmp_60
    FROM {{source('tablones_analisis','tablon_rfmp_v2')}}  --{{source('tablones_analisis','tablon_rfmp')}}
    ),
    
  usuarios_invitaygana as (
    SELECT DISTINCT
      referrer uuid
    FROM {{ ref('parejas_iyg') }}  i
    )
  
SELECT DISTINCT
  id,
  FORMAT_DATE("%Y-%m-01", DATE(ob_completed_at)) camada,
  state,
  CASE WHEN ultimo_segmento_rfmp_60 is null THEN 'unknown' ELSE ultimo_segmento_rfmp_60 END AS segmento_rfmp,
  CASE WHEN uuid is not null THEN true else false end as invited
FROM  {{ ref('users_tenpo') }} u
LEFT JOIN last_rfmp USING(id)
LEFT JOIN usuarios_invitaygana on uuid = u.id
WHERE 
  state in (4,7,8,21,22)
  AND ob_completed_at is not null
ORDER BY 1 ASC, 2 ASC 