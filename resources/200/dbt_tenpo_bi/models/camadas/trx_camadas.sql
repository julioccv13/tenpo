{% set partitions_to_replace = [
    'date_trunc(current_date,month)',
    'date_sub(date_trunc(current_date,month), interval 1 month)',
    'date_sub(date_trunc(current_date,month), interval 2 month)'
] %}

{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'mes', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}

WITH
  users as (
    select distinct
      id as user,
      state,
      DATE(ob_completed_at, "America/Santiago") fecha_ob,
      date_trunc(DATE(ob_completed_at, "America/Santiago"),month) as mes_ob
    from {{ref('users_tenpo')}}
  ),
  economics_app AS (
    SELECT distinct
      user,
      date_trunc(fecha,month) as mes,
      FORMAT_DATE('%G%V', fecha) semana,
      sum(monto) monto_gastado,
      count(distinct trx_id) cuenta_trx,
      FROM {{ref('economics')}} --`tenpo-bi-prod`.`economics`.`economics`  
      WHERE
        linea not in ('reward', 'aum_savings')
        AND nombre not like "%Devolución%"
{% if is_incremental() %}
        and date_trunc(fecha,month) in ({{ partitions_to_replace | join(',') }})
{% endif %}
      GROUP BY user,mes,semana
    ),
  trx_por_mes as(
    SELECT 
      user, 
      fecha_ob, 
      mes_ob,
      mes,
      monto_gastado, 
      cuenta_trx
    FROM economics_app
    JOIN  users using (user)  --`tenpo-bi-prod`.`users`.`users_tenpo`
    WHERE 
      mes_ob <= mes 
      AND state in (4,7,8,21,22)
    )
 SELECT 
  * 
FROM trx_por_mes