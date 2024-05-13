{% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval 1 day)',
    'date_sub(current_date, interval 2 day)'
] %}

{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'Fecha_Fin_Analisis_DT', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}

WITH 
    coalesced_data as (
      SELECT 
        * EXCEPT(coalesced_balance_amount)
        ,coalesced_balance_amount coalesce_balance_amount
        ,coalesced_f_actividad_app coalesced_f_actividad
        ,coalesce(balance_amount, last_value(balance_amount ignore nulls) over (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT  rows between unbounded preceding and current row), 0) as coalesced_balance_amount
        ,coalesce(f_actividad_app, last_value(f_actividad_app ignore nulls) over (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT rows between unbounded preceding and current row)) as coalesced_f_actividad_app
        ,coalesce(periodo, last_value(periodo ignore nulls) over (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT rows between unbounded preceding and current row)) as coalesced_periodo
        ,coalesce(ultimo_evento_periodo_es_churn, last_value(ultimo_evento_periodo_es_churn ignore nulls) over (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT rows between unbounded preceding and current row)) as coalesce_churn
        ,max(cierre_cuenta) over (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT rows between unbounded preceding and current row) as cierre_de_cuenta_pasado
      FROM {{source('churn','tablon_daily_eventos_churn')}}  
      WHERE TRUE
      {% if is_incremental() %}
        AND Fecha_Fin_Analisis_DT in ({{ partitions_to_replace | join(',') }})
      {% endif %}
        ), 
    lagged_data as (
      SELECT 
        coalesced_data.*
        ,lag(coalesce_churn) over (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ) as churn_periodo_pasado
      FROM coalesced_data
      ), 
    retornos as (
      SELECT 
        lagged_data.*
        ,case when churn_periodo_pasado = 1 and coalesced_churn = 0 then 1 else 0 end as retorno_periodo
      FROM lagged_data
      ),
    statet as (
      SELECT
          retornos.*
          ,case when cierre_cuenta = 1 then 'cuenta_cerrada'
          when coalesce(ultimo_evento_periodo_es_churn, churn_periodo_pasado, 0) = 1 and coalesce( coalesce_balance_amount , 0) < 1000 then 'churneado'
          else 'activo' end as state
      FROM retornos
      ),
    last_statet as (
        SELECT
            *
            ,coalesce(lag(state) over (partition by user order by Fecha_Fin_Analisis_DT ), 'onboarding') as last_state
        FROM statet
      )

    SELECT
      Fecha_Fin_Analisis_DT 
      ,user   
      ,state
      ,last_state   
      ,cierre_involuntario
      ,case when state != "activo" then true else false end as churn --churn = 0 es cliente_ready
      ,case when state != "cuenta_cerrada" then true else false end as cliente
      ,case when state = "cuenta_cerrada" then true else false end as cierre_cuenta
      ,case when state != "activo" and cierre_involuntario is false then true else false end as churn_voluntario
    FROM last_statet