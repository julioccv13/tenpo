{{ config(materialized='table') }}

WITH
  tenpo_paypal AS (
            select 
            id,
            rut,
            IF(clientes_tenpo.email IS NOT NULL,clientes_tenpo.email,clientes_paypal.email) as email,
            clientes_tenpo.state,
            clientes_tenpo.ts_creacion_tenpo,
            clientes_paypal.ts_creacion_paypal,
            clientes_paypal.last_serv_paypal,
            clientes_paypal.last_login,
            clientes_paypal.tipo_usuario,
            clientes_paypal.last_trx_pp,
            clientes_tenpo.phone,
            clientes_tenpo.ts_ob_tenpo
            from 
            {{ ref('clientes_tenpo') }} 
            FULL JOIN 
            {{ ref('clientes_paypal') }}
            using (RUT)
  ), joined_data as (
        SELECT
         *,
         CASE 
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NOT NULL AND ts_creacion_tenpo IS NOT NULL THEN 'Tenpo + Topups + Paypal'
          WHEN ts_creacion_paypal IS NULL AND ts_creacion_topup IS NOT NULL AND ts_creacion_tenpo IS NOT NULL THEN 'Tenpo + Topups'
          WHEN ts_creacion_paypal IS NULL AND ts_creacion_topup IS NULL AND ts_creacion_tenpo IS NOT NULL THEN 'Tenpo'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NULL AND ts_creacion_tenpo IS NOT NULL THEN 'Tenpo + Paypal'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NOT NULL AND ts_creacion_tenpo IS NULL THEN 'Topups + Paypal'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NULL AND ts_creacion_tenpo IS NULL THEN 'Paypal'
          WHEN ts_creacion_paypal IS NULL AND ts_creacion_topup IS NOT NULL AND ts_creacion_tenpo IS NULL THEN 'Topups'
          ELSE 'Nuevo'
          END as services,
        CASE
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NOT NULL AND last_login <= last_topup THEN 'Paypal'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NOT NULL AND last_login > last_topup  THEN 'Topups'
          WHEN ts_creacion_paypal IS NULL AND ts_creacion_topup IS NOT NULL THEN 'Topups'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NULL THEN 'Paypal'
          ELSE 'Nuevo'
          END AS last_ndd_service,
        CASE
          WHEN last_pp_use IS NOT NULL AND last_tu_bm IS NOT NULL AND last_pp_use <= last_tu_bm THEN 'Paypal'
          WHEN last_pp_use IS NOT NULL AND last_tu_bm IS NOT NULL AND last_pp_use > last_tu_bm  THEN 'Topups'
          WHEN last_pp_use IS NULL AND last_tu_bm IS NOT NULL THEN 'Topups'
          WHEN last_pp_use IS NOT NULL AND last_tu_bm IS NULL THEN 'Paypal'
          ELSE 'Nuevos'
          END AS last_ndd_service_bm,
        CASE
          WHEN last_pp_use IS NOT NULL AND last_tu_bm IS NOT NULL AND last_pp_use <= last_tu_bm THEN actividad_pp_bm
          WHEN last_pp_use IS NOT NULL AND last_tu_bm IS NOT NULL AND last_pp_use > last_tu_bm  THEN actividad_tu_bm
          WHEN last_pp_use IS NULL AND last_tu_bm IS NOT NULL THEN actividad_tu_bm
          WHEN last_pp_use IS NOT NULL AND last_tu_bm IS NULL THEN actividad_pp_bm
          ELSE 'Nuevos'
          END AS actividad_bm,
         CASE
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NOT NULL AND ts_creacion_paypal <= ts_creacion_topup THEN 'Paypal'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NOT NULL AND ts_creacion_paypal > ts_creacion_topup  THEN 'Topups'
          WHEN ts_creacion_paypal IS NULL AND ts_creacion_topup IS NOT NULL THEN 'Topups'
          WHEN ts_creacion_paypal IS NOT NULL AND ts_creacion_topup IS NULL THEN 'Paypal'
          ELSE 'Nuevo'
          END AS fisrt_ndd_service,
         CASE 
            WHEN ts_creacion_tenpo IS NULL THEN 'Por migrar'
            WHEN ts_creacion_tenpo IS NOT NULL AND (last_login IS NULL) AND (last_topup IS NULL) THEN 'Tenpista'
            WHEN ts_creacion_tenpo IS NOT NULL AND (ts_creacion_tenpo>last_login OR last_login IS NULL) AND (ts_creacion_tenpo>last_topup OR last_topup IS NULL) THEN 'Migrado'
            WHEN ts_creacion_tenpo IS NOT NULL AND ts_creacion_tenpo<=last_login THEN 'Retorno Paypal'
            WHEN ts_creacion_tenpo IS NOT NULL AND ts_creacion_tenpo<=last_topup THEN 'Retorno Topup'
            ELSE NULL
          END as status_migracion,
        FROM {{ ref('clientes_topups') }}
         FULL JOIN tenpo_paypal as t1
         on (email=email_tu)
         LEFT JOIN {{ ref('last_service_bm') }} USING (id)
  ), ordered_data as (
    SELECT 
    *
    ,ROW_NUMBER() OVER (PARTITION BY rut) as ro_no
    FROM joined_data
  )

select 
* except (ro_no)
from ordered_data
where ro_no = 1