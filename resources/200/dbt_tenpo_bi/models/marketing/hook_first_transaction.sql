{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}



WITH rewards AS 
(SELECT 
      DISTINCT
      DATE(e.trx_timestamp  , "America/Santiago") AS fecha,
      e.trx_timestamp,
      e.nombre,
      CASE 
        WHEN e.comercio LIKE '%invita y gana%' THEN (e.monto*2)
        WHEN LOWER(e.comercio) LIKE '%rata%' THEN (e.monto + 2000)
        WHEN LOWER(e.comercio) LIKE '%vela%' THEN (e.monto + 1500)  
       ELSE e.monto END AS monto,
      valor_dolar_cierre,
      e.trx_id,
      --prp.id_tx_externo, --cross this with r.id from reedeems
      e.user,
      e.linea,
      e.comercio,
      ROW_NUMBER() OVER (PARTITION BY user ORDER BY trx_timestamp ASC ) AS row
--FROM {{ref('economics')}} e
FROM {{ ref('economics') }} e
--JOIN {{source('prepago','prp_movimiento')}} prp on prp.uuid = e.trx_id
LEFT JOIN (SELECT 
   fecha,
   IF(valor_dolar_cierre is null, LAG(valor_dolar_cierre) OVER (ORDER BY fecha), 
   IF( (LAG(valor_dolar_cierre) OVER (ORDER BY fecha)) IS NULL, 714, valor_dolar_cierre )) valor_dolar_cierre
FROM {{ref('dolar')}}
order by fecha desc
LIMIT 1) ON 1 = 1
WHERE linea = 'reward'
AND lower(comercio) NOT LIKE '%si realizas una carga%'
----AND e.trx_id NOT IN (SELECT distinct trx_id FROM {{ref('hook_first_cashin')}}) --exclude first cash in coupons
ORDER BY user DESC, trx_timestamp DESC)

SELECT DISTINCT
        IFNULL(B.motor,'desconocido') motor,
        rewards.fecha,
        rewards.trx_timestamp,
        rewards.user,
        rewards.nombre,
        rewards.comercio as premio,
        rewards.monto,
       CASE 
            WHEN valor_dolar_cierre IS null THEN round(monto/714) 
        ELSE round(monto/valor_dolar_cierre) END AS monto_usd,
FROM rewards
LEFT JOIN  {{ ref('appsflyer_users') }} B on rewards.user = B.customer_user_id
WHERE row = 1
-- AND rewards.trx_id not in (select distinct 
--                                     trx_id 
--                             FROM {{ref('hook_first_cashin')}}
--                             where trx_id is not null)
ORDER BY 1 DESC
