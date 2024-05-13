{{ 
    config(
          materialized='table',  
          tags=["business_dashboard", "daily", "bi"]
          )
}} 

WITH 
    tabla_base as (
        SELECT distinct
          DATE_TRUNC(dia, month) mes
          ,canal
          ,linea
          ,tipo_usuario
          ,categoria
          ,moneda
        FROM UNNEST(GENERATE_DATE_ARRAY('2017-01-01', current_date("America/Santiago"), INTERVAL 1 day)) AS dia
        CROSS JOIN UNNEST(['mastercard', 'mastercard_physical','p2p', 'crossborder', 'utility_payments','top_ups','paypal']) AS linea
        CROSS JOIN UNNEST(['gpv','trx','usr']) AS categoria
        CROSS JOIN UNNEST(['generico','abono','retiro']) AS tipo_usuario
        CROSS JOIN UNNEST(['clp','usd']) AS moneda
        CROSS JOIN UNNEST(['web','app']) AS canal
            ),
     consolidado_categorias as (
        SELECT 
          mes
          ,canal
          ,linea
          ,tipo_usuario
          ,'gpv' categoria
          ,'clp' moneda
          ,sum(gpv_clp) valor
        FROM {{ ref('consolidado_economics_app_web') }} a
        GROUP BY 
          mes,canal,linea,tipo_usuario
     
       
        union all
        
        SELECT 
          mes
          ,canal
          ,linea
          ,tipo_usuario
          ,'gpv' categoria
          ,'usd' moneda
          ,sum( gpv_usd ) valor
        FROM {{ ref('consolidado_economics_app_web') }} a --`tenpo-bi-prod.corporative.consolidado_economics_app_web` a
        GROUP BY 
          mes,canal,linea,tipo_usuario
    
        
        union all
        
        SELECT 
          mes
          ,canal
          ,linea
          ,tipo_usuario
          ,'trx' categoria
          ,'clp' moneda
          ,count( distinct trx ) valor
        FROM {{ ref('consolidado_economics_app_web') }} a --`tenpo-bi-prod.corporative.consolidado_economics_app_web` a
        GROUP BY 
          mes,canal,linea,tipo_usuario
          
       union all
       
        SELECT 
          mes
          ,canal
          ,linea
          ,tipo_usuario
          ,'usr' categoria
          ,'clp' moneda
          ,count( distinct usr ) valor
        FROM {{ ref('consolidado_economics_app_web') }} a --`tenpo-bi-prod.corporative.consolidado_economics_app_web` a
        GROUP BY 
          mes,canal,linea,tipo_usuario
          ),
      presupuesto_mensual as (
        SELECT
         CAST(mes as DATE) mes
         ,canal
         ,linea
         ,tipo_usuario
         ,categoria
         ,moneda
         ,presupuesto
        FROM {{ ref('presupuesto_mensual') }} a --`tenpo-bi-prod.corporative.presupuesto_mensual` 
        ),
    tabla_unificada as (
        SELECT
          EXTRACT(year from MES) year 
          ,tabla_base.mes
          ,tabla_base.canal
          ,tabla_base.linea
          ,tabla_base.tipo_usuario
          ,tabla_base.categoria
          ,tabla_base.moneda
          ,CASE 
            WHEN DATE_TRUNC(CURRENT_DATE("America/Santiago"), MONTH) = mes 
            THEN EXTRACT(DAY FROM DATE_SUB(DATE_TRUNC(DATE_ADD(mes, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY))/EXTRACT(DAY FROM CURRENT_DATE("America/Santiago"))
            ELSE 1
            END as multiplicador
          ,valor valor_mes
          ,presupuesto presupuesto_mes
        FROM tabla_base
        FULL JOIN consolidado_categorias USING(mes, canal ,linea,tipo_usuario,categoria,moneda)
        LEFT JOIN presupuesto_mensual USING(mes, canal ,linea,tipo_usuario,categoria,moneda)
        WHERE 
          (valor is not null or presupuesto is not null)
        ORDER BY
          mes desc, linea desc
          ), 
    calculo_ytd as (
        SELECT 
          t.*, 
          LAG(t.valor_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal ORDER BY t.mes) as last_month, 
          SUM(t.valor_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, t.year ORDER BY t.mes) as year_to_date,
          SUM(t.presupuesto_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, t.year ORDER BY t.mes) as acc_target
        FROM tabla_unificada t
        ),

      calculo_last_ytd as (
        SELECT 
          t.*, 
          LAG(t.year_to_date) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, EXTRACT(MONTH FROM CAST(mes AS DATE))  ORDER BY t.year) as last_year_to_date,
          LAG(t.valor_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, EXTRACT(MONTH FROM CAST(mes AS DATE))  ORDER BY t.year) as month_last_year
        FROM calculo_ytd t
        )

SELECT 
  *
FROM calculo_last_ytd