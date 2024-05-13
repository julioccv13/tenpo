{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "user",
    partition_by = {'field': 'FECHA_CONCILIACION', 'data_type': 'timestamp'}
  ) 
}}

WITH 
  datos_conciliaciones_historicas as(
    SELECT 
      CAST(ID_PREPAGO AS INT64) ID_PREPAGO , 
      CAST(ID_CONTABILIDAD AS INT64) ID_CONTABILIDAD, 
      CAST(ID_TRX AS INT64) ID_TRX, 
      CAST(CONTRATO_TRX AS INT64) CONTRATO_TRX,
      UUID_TRX,
      CAST(ID_CUENTA_ORIGEN AS STRING) ID_CUENTA_ORIGEN , 
      TIPO_TRX, 
      MOV_CONTABLE, 
      CAST(FECHA_TRX AS TIMESTAMP) FECHA_TRX, 
      CAST(FECHA_CONCILIACION AS TIMESTAMP) FECHA_CONCILIACION , 
      CAST(MONTO_TRX_INGRESO as INT64) MONTO_TRX_INGRESO,
      CAST(MONTO_TRX_PERDIDA as INT64) MONTO_TRX_PERDIDA,
      CAST(MONTO_TRX_MCARD_PESOS as INT64) MONTO_TRX_MCARD_PESOS,
      CAST(TASA_INTERCAMBIO AS FLOAT64) TASA_INTERCAMBIO ,
      MONTO_TRX_USD,       
      VALOR_USD,
      RUT_TRX,
      EXTERNAL_ID_TRX
    FROM {{source('aux_table','trx_conciliadas')}}
    WHERE FECHA_CONCILIACION < "2021-01-01"
    ),
  datos_conciliaciones as (
   SELECT 
    ID_PREPAGO, 
    ID_CONTABILIDAD,       
    ID_TRX, 
    SAFE_CAST(CONTRATO_TRX AS INT64) CONTRATO_TRX,
    UUID_TRX,
    ID_CUENTA_ORIGEN, 
    TIPO_TRX, 
    MOV_CONTABLE, 
    FECHA_TRX, 
    FECHA_CONCILIACION, 
    MONTO_TRX_INGRESO, 
    MONTO_TRX_PERDIDA, MONTO_TRX_MCARD_PESOS, 
    TASA_INTERCAMBIO,
    MONTO_TRX_USD,       
    VALOR_USD,
    RUT_TRX,
    EXTERNAL_ID_TRX
   FROM {{source('reconciliation','trx_part_conciliadas')}}
   WHERE FECHA_CONCILIACION >= "2021-01-01"
   ),   
  datos_conciliaciones_union as (
    SELECT
      *,
      row_number() over (partition by ID_TRX order by FECHA_CONCILIACION desc) as row_no_actualizacion 
    FROM (
      SELECT * FROM datos_conciliaciones_historicas 
      UNION ALL 
      SELECT * FROM datos_conciliaciones 
      )
    ),   
  datos_dedup as (
    SELECT DISTINCT
      * EXCEPT(row_no_actualizacion)
    FROM datos_conciliaciones_union
    WHERE row_no_actualizacion = 1
    ),
  usuarios_tenpo as (
    SELECT DISTINCT
      id
     ,tributary_identifier
     ,row_number() over (partition by id order by updated_at desc) as row_num_actualizacion
    FROM {{source('tenpo_users','users')}}
    WHERE 
      state in (4,7,8,21,22)
  ), dolar_price as (
      SELECT 
      fecha
      ,coalesce(valor_dolar_cierre, last_value(valor_dolar_cierre ignore nulls) over (order by fecha)) as usd_to_clp
      FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE)) as fecha
      LEFT JOIN {{ref('dolar')}}dolar
      ON fecha = dolar.fecha
  ), modified_data as (
      SELECT
      *
      ,MONTO_TRX_MCARD_PESOS/usd_to_clp as monto_usd
      ,case when TIPO_TRX in ('COMPRA_NACIONAL','SUSCRIPCION_NACIONAL', 'COMPRA_PESOS', 'SUSCRIPCION_PESOS') then 'clp'
        when TIPO_TRX in ( 'COMPRA_OTRA_MONEDA', 'SUSCRIPCION') then 'no_clp'
        end as tipo_moneda_trx
      ,case when TIPO_TRX in ('COMPRA_NACIONAL','SUSCRIPCION_NACIONAL') then 'nacional'
        when TIPO_TRX in ('COMPRA_PESOS', 'COMPRA_OTRA_MONEDA', 'SUSCRIPCION_PESOS', 'SUSCRIPCION') then 'internacional'
        end as tipo_trx_mc
      FROM datos_dedup a 
      LEFT JOIN dolar_price b
      ON date(a.fecha_trx, 'America/Santiago') = b.fecha
  )
 
SELECT 
  r.tributary_identifier
  ,id user
  ,modified_data.* EXCEPT(RUT_TRX)
  ,case when tipo_trx_mc = 'internacional' and monto_usd < 5 then 'intl_tier_1'
        when tipo_trx_mc = 'internacional' and monto_usd < 25 then 'intl_tier_2'
        when tipo_trx_mc = 'internacional' and monto_usd >= 25 then 'intl_tier_3'
        when tipo_trx_mc = 'nacional' and monto_usd < 1 then 'dom_tier_1'
        when tipo_trx_mc = 'nacional' and monto_usd < 5 then 'dom_tier_2'
        when tipo_trx_mc = 'nacional' and monto_usd < 25 then 'dom_tier_3'
        when tipo_trx_mc = 'nacional' and monto_usd < 100 then 'dom_tier_4'
        when tipo_trx_mc = 'nacional' and monto_usd >= 100 then 'dom_tier_5'
        end as switching_tier
FROM modified_data 
JOIN {{source('identity','ruts')}} r ON RUT_TRX = rut_complete
JOIN usuarios_tenpo u ON LOWER(r.tributary_identifier) = LOWER(u.tributary_identifier )
WHERE 
  row_num_actualizacion = 1