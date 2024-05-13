{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
    economics AS(
      SELECT
        *
        ,CASE WHEN linea = 'cash_in' THEN dt_trx_chile ELSE null end as fecha_cashin 
      FROM {{ ref('economics') }} --`tenpo-bi-prod.economics.economics` 
      WHERE 
        linea not in ('reward', 'aum_savings', 'cash_out')
        and nombre not like '%Devolución%'
        ),
    dates AS (
      SELECT 
        CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha
      FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)) dia
      ORDER BY 
        dia DESC
        ),
    lagged_data_cashin as (
       SELECT
         user
         ,dt_trx_chile 
         ,linea
         ,trx_id
         ,tipo_trx
         ,LAG(fecha_cashin) OVER(PARTITION BY user ORDER BY dt_trx_chile  ) fecha_cashin_anterior
       FROM economics
       ),
    fill_last_cashin_datetime as (
       SELECT
        * EXCEPT(fecha_cashin_anterior),  
        IF (fecha_cashin_anterior is null,  LAST_VALUE(fecha_cashin_anterior ignore nulls) OVER (PARTITION BY  user ORDER BY  dt_trx_chile  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING ), fecha_cashin_anterior) fecha_ult_cashin
       FROM lagged_data_cashin
       ), 
    count_trx_in_between as (
       SELECT
         *
         ,sum(case when linea != 'cash_in' then 1 else 0 end) OVER (PARTITION BY user, fecha_ult_cashin ORDER BY dt_trx_chile RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cuenta_trx
       FROM fill_last_cashin_datetime
       )
     
 
SELECT
* EXCEPT(cuenta_trx)
,lag(cuenta_trx) OVER(PARTITION BY user ORDER BY fecha_ult_cashin) trx_previas
FROM count_trx_in_between
WHERE TRUE
QUALIFY 
    linea = "cash_in"