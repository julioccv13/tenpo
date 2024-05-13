{{ 
  config(
    tags=["hourly", "paypal","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT15', 'tenpo-datalake-sandbox')
  ) 
}}

with base as (
    SELECT distinct 
        mail,
        saldo_ppal,
        saldo_secd
    FROM {{source('drive_sheets','saldos_paypal_tbl')}}
    where mail like '%@%'
)
SELECT DISTINCT
  t1.mail,
  t2.id is not null as en_tenpo,
  t2.id,
  case 
      when t2.state in (4) THEN 'vigente'
      when t2.state in (7,8,21,22) THEN 'cerrada'
      when t2.id is null THEN 'sin cuenta'
      else 'incompleta'
  end as state,
  saldo_ppal,
  saldo_secd
FROM base t1
LEFT JOIN {{source('tenpo_users','users')}} t2 ON (lower(t1.mail)=lower(t2.email))