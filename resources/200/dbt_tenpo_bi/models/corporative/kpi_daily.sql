{{ 
    config(
          materialized='table',  
          tags=["business_dashboard", "daily", "bi"]
          )
}} 

WITH 
    tabla_base as (
        SELECT distinct
          dia fecha
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
          dia fecha
          ,canal
          ,linea
          ,tipo_usuario
          ,'gpv' categoria
          ,'clp' moneda
          ,sum(gpv_clp) valor
        FROM {{ ref('consolidado_economics_app_web') }} a
        GROUP BY 
          dia,canal,linea,tipo_usuario
     
       
        union all
        
        SELECT 
          dia fecha
          ,canal
          ,linea
          ,tipo_usuario
          ,'gpv' categoria
          ,'usd' moneda
          ,sum( gpv_usd ) valor
        FROM {{ ref('consolidado_economics_app_web') }} a --`tenpo-bi-prod.corporative.consolidado_economics_app_web` a
        GROUP BY 
          dia,canal,linea,tipo_usuario
    
        
        union all
        
        SELECT 
          dia fecha
          ,canal
          ,linea
          ,tipo_usuario
          ,'trx' categoria
          ,'clp' moneda
          ,count( distinct trx ) valor
        FROM {{ ref('consolidado_economics_app_web') }} a --`tenpo-bi-prod.corporative.consolidado_economics_app_web` a
        GROUP BY 
          dia,canal,linea,tipo_usuario
          
       union all
       
        SELECT 
          dia fecha
          ,canal
          ,linea
          ,tipo_usuario
          ,'usr' categoria
          ,'clp' moneda
          ,count( distinct usr ) valor
        FROM {{ ref('consolidado_economics_app_web') }} a --`tenpo-bi-prod.corporative.consolidado_economics_app_web` a
        GROUP BY 
          dia,canal,linea,tipo_usuario
          ),
       presupuesto_diario as (
        SELECT
         CAST(fecha as DATE) fecha
         ,canal
         ,linea
         ,tipo_usuario
         ,categoria
         ,moneda
         ,presupuesto
        FROM {{ ref('presupuesto_diario') }} a --`tenpo-bi-prod.corporative.presupuesto_diario` 
        )
          
          
      SELECT 
        tabla_base.*
        ,DATE_TRUNC(tabla_base.fecha, MONTH) mes
        ,DATE_TRUNC(tabla_base.fecha, YEAR) year
        ,valor
        ,presupuesto
      FROM tabla_base
      FULL JOIN consolidado_categorias USING(fecha, canal ,linea,tipo_usuario,categoria,moneda)
      LEFT JOIN presupuesto_diario USING(fecha, canal ,linea,tipo_usuario,categoria,moneda)
      WHERE (valor is not null or presupuesto is not null)
      ORDER BY
        fecha desc, linea desc