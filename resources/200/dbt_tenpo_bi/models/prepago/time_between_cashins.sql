{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  economics AS(
      SELECT 
        distinct user, fecha ,trx_timestamp, tipo_trx, tipofac, monto, trx_id
      FROM {{ ref('economics') }} --`tenpo-bi-prod.economics.economics` 
      WHERE 
        linea = 'cash_in'
      ),
  dates AS (
      SELECT
        CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha
      FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)) dia
      ORDER BY dia DESC
      ),
   crossjoin_fechas_cargas as (
        SELECT DISTINCT
          user,
          dates.fecha fecha,
        FROM economics
        CROSS JOIN dates 
        WHERE 
          economics.fecha <= dates.fecha
        ORDER by 
          user, dates.fecha 
          ),
   cargas_x_user as (
      SELECT 
        user,
        fecha,
        monto carga,
        LAG(monto) OVER (PARTITION BY user ORDER BY fecha ASC) carga_anterior,
        TIMESTAMP_DIFF(trx_timestamp, LAG(trx_timestamp) OVER (PARTITION BY user ORDER BY trx_timestamp ASC), HOUR) dif_horas
      FROM crossjoin_fechas_cargas
      LEFT JOIN economics USING(user,fecha)
      ),
    lagged_data as (
      SELECT
        user
        ,fecha
        ,CASE WHEN carga is null then last_value(carga ignore nulls) over (partition by user order by fecha rows between unbounded preceding and current row ) else carga end as carga
        ,CASE WHEN carga_anterior is null then last_value(carga_anterior ignore nulls) over (partition by user order by fecha rows between unbounded preceding and current row ) else carga_anterior end as carga_anterior
        ,CASE WHEN dif_horas is null then last_value(dif_horas ignore nulls) over (partition by user order by fecha rows between unbounded preceding and current row ) else dif_horas end as dif_horas
      FROM cargas_x_user
    ),
    avg_dif as(
      SELECT DISTINCT
        fecha,
        user,
        AVG(dif_horas) dif_hrs_prom_dia
      FROM lagged_data
      WHERE
        dif_horas is not null
      GROUP BY 
        fecha, user
    )
SELECT 
  *
FROM avg_dif 