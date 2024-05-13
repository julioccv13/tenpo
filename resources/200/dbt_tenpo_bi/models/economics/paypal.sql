  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}

{{ 
  config(
    materialized='table',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    enabled=True
  ) 
}}


with
data_app as (
  SELECT distinct transaction_id,user_id from {{source('paypal','data_consolidation')}}
  union all
  SELECT distinct transaction_id,t1.user_id from {{source('paypal_payments','transactions')}} t1 
  left join {{source('paypal','data_consolidation')}}  t2 using (transaction_id)
  where t2.transaction_id is null
)

SELECT DISTINCT
    DATE(trx.fec_fechahora_envio , "UTC") as fecha 
    ,trx.fec_fechahora_envio as trx_timestamp
    ,case 
      when tip_trx = 'RETIRO_APP_PAYPAL' then 'Retiro Paypal' 
      when tip_trx = 'ABONO_APP_PAYPAL' then 'Abono Cuenta USD' 
      end as nombre
    ,trx.mto_monto_dolar*valor_dolar_multicaja as monto
    ,cast(trx.id as string) as trx_id
    ,dc.user_id as user
    ,case 
      when tip_trx = 'RETIRO_APP_PAYPAL' then 'paypal' 
      when tip_trx = 'ABONO_APP_PAYPAL' then 'paypal_abonos' 
      end as linea
    ,'app' as canal
    ,'n/a' as comercio
    ,CAST(null as STRING) as id_comercio
    ,CAST(null as STRING) as actividad_cd
    ,CAST(null as STRING) as actividad_nac_cd
    ,CAST(null AS FLOAT64) as codact 
    ,null as tipofac
FROM {{source('paypal','pay_transaccion')}} trx
  JOIN data_app dc on (trx.id =dc.transaction_id)
WHERE 
  est_estado_trx IN (2,8,17,24) 
  AND tip_trx IN ("RETIRO_APP_PAYPAL","ABONO_APP_PAYPAL")
QUALIFY
  row_number() over (partition by CAST(trx.id AS STRING) order by trx.fec_fechahora_actualizacion desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}

