{{ config(materialized='ephemeral',  tags=["daily", "bi"]) }}

SELECT
  DATE(fec_fechahora_envio , "America/Santiago") AS dia,
  DATE_TRUNC(DATE(fec_fechahora_envio , "America/Santiago"), ISOWEEK) semana,
  DATE_TRUNC(DATE(fec_fechahora_envio , "America/Santiago"), MONTH) mes,
  CAST(mto_monto_dolar as FLOAT64) * CAST(valor_dolar_multicaja AS FLOAT64) as gpv_clp,
  CAST(mto_monto_dolar AS FLOAT64) gpv_usd,
  CAST(codigo_mc as STRING) trx,
  CAST(id_cuenta as STRING) usr,
  'paypal' as linea,
  'web' as canal,
  CASE 
   WHEN tip_trx  = "ABONO_PAYPAL" THEN 'abono'
   ELSE 'retiro' 
   END AS tipo_usuario,
FROM {{ '`'+env_var('DBT_PROJECT25', 'tenpo-datalake-sandbox')+'.paypal_2020.pay_transaccion`' }}
WHERE 
 est_estado_trx IN (2,3,8,17,24) 
 AND tip_trx IN ("ABONO_PAYPAL","RETIRO_PAYPAL","RETIRO_USD_PAYPAL")
QUALIFY 
    row_number() over (partition by CAST(codigo_mc AS STRING) order by fec_fechahora_actualizacion desc) = 1