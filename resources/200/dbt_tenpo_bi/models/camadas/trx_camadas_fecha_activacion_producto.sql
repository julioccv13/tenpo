{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  economics_app AS (
    SELECT 
      user, 
      linea,
      FORMAT_DATE('%Y-%m-01', fecha) mes,
      EXTRACT(YEAR FROM fecha)*12 + EXTRACT(MONTH FROM fecha) meses,
      SUM(monto) monto_gastado, 
      count(distinct trx_id) cuenta_trx
    FROM {{ ref('economics') }} a
    JOIN {{ ref('users_tenpo') }} b 
    on a.user = b.id  --{{ref('users_tenpo')}}
    WHERE 
      FORMAT_DATE('%Y-%m-01',DATE(ob_completed_at, "America/Santiago")) <= FORMAT_DATE('%Y-%m-01', fecha) 
      AND state in (4,7,8,21,22)
      AND linea NOT IN ('p2p_received','aum_savings')
    GROUP BY 
      user,linea, mes,meses 
    ORDER BY
      user, mes DESC 
)
  
SELECT 
  pivote.user
  ,pivote.linea
  ,pivote.mes as mes_analisis
  ,siguiente.mes as mes_proximo
  ,siguiente.meses - pivote.meses as diferencia_meses
  ,siguiente.monto_gastado
  ,siguiente.cuenta_trx 
FROM economics_app pivote
  LEFT JOIN economics_app siguiente
  ON pivote.user = siguiente.user and pivote.linea = siguiente.linea
WHERE 
  pivote.mes <= siguiente.mes
