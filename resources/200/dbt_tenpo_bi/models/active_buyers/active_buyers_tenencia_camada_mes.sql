
{{   config(  tags=["daily", "bi"], materialized='table') }}


SELECT
  camada
  ,mes
  ,AVG(tenencia_mes) tenencia_promedio
  ,count(distinct user) usr
FROM(
  SELECT DISTINCT
    t.user,
    date_trunc(e.fecha, month) mes,
    date_trunc(date(ob_completed_at, "America/Santiago") , month) camada,
    max(uniq_productos_simplif_origen)  tenencia_mes,
  FROM {{source('productos_tenpo','tenencia_productos_tenpo')}} t -- `tenpo-bi.productos_tenpo.tenencia_productos_tenpo` t --
  JOIN {{ ref('economics') }} e on t.user = e.user  AND fecha <= Fecha_Fin_Analisis_DT -- {{ ref('economics') }}  USING(user)
  JOIN {{ ref('users_tenpo') }}  on id = t.user AND date(ob_completed_at, "America/Santiago") <= Fecha_Fin_Analisis_DT --{{source('tenpo_users','users')}}
  WHERE 
    fecha >=  date_sub(Fecha_Fin_Analisis_DT,interval 29 day)  
    AND fecha <= Fecha_Fin_Analisis_DT
    AND uniq_productos_simplif_origen > 0
    AND lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
  GROUP BY 1,2,3
  ORDER BY 2 desc
)
GROUP BY camada, mes
ORDER BY mes desc, camada desc