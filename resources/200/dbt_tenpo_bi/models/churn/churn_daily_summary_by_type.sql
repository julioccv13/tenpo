{{ config(tags=["daily", "bi"], materialized='table') }}

WITH 
  datos as (
    SELECT
       Fecha_Fin_Analisis_DT
       ,user
       ,case when state = "cuenta_cerrada"  then true else false end f_cuenta_cerrada
       ,case when state != "cuenta_cerrada" then true else false end  f_cliente
       ,case when state = "activo" then true else false end f_cliente_ready
    FROM {{ ref('daily_churn') }} --`tenpo-bi-prod.churn.daily_churn`
    )
SELECT
  Fecha_Fin_Analisis_DT 
  ,f_cliente
  ,f_cliente_ready
  ,f_cuenta_cerrada
  ,case 
   when f_cuenta_cerrada then 'Cuenta Cerrada'
   when f_cliente_ready  then 'Cliente Ready'  
   else  'Desinstalaciones'
   end as tipo
  ,count(distinct user) cuenta
FROM datos
GROUP BY 1,2,3,4