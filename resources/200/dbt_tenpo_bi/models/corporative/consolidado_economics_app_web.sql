 
{{ config(materialized='table',  tags=["daily", "bi"]) }}
WITH
  economics as (
    SELECT dia,semana,mes,canal,linea,tipo_usuario,gpv_clp,gpv_usd,trx,usr
    FROM {{ ref('paypal_web') }}
    UNION ALL
    SELECT dia,semana,mes,canal,linea,tipo_usuario,gpv_clp,gpv_usd,trx,usr
    FROM {{ ref('topups_web') }}
    UNION ALL
    SELECT dia,semana,mes,canal,linea,tipo_usuario,gpv_clp,gpv_usd,trx,usr
    FROM {{ ref('tenpo_app') }}
    )
SELECT * FROM economics