{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH users as (
    SELECT DISTINCT
      id as user
      ,DATE_TRUNC(date(ob_completed_At), MONTH) as mes_ob
      ,date(ob_completed_At,"America/Santiago") as day_ob
    FROM {{ ref('users_tenpo') }} --`tenpo-bi-prod.users.users_tenpo`

  ), economics_mau as (
    SELECT 
      fecha
      ,user
      ,max(linea = 'reward') as f_reward
      ,max(linea <> 'reward' and nombre not like '%Devol%') as f_dau
    FROM {{ ref('economics') }} a
    group by 1, 2
    
  )


SELECT 
    a.user
    ,b.day_ob
    ,a.fecha
    ,a.f_dau
    ,a.f_reward
    ,case when b.day_ob = a.fecha and coalesce(f_dau, false) and coalesce(f_reward, false) then '1. OB day con premio' -- El premio deberia ser por mes o por dia?
        when b.day_ob = a.fecha and coalesce(f_dau, false) and not coalesce(f_reward, false)  then '2. OB day sin premio'
        when coalesce(f_dau, false) and coalesce(f_reward, false) then '3. No OB day con premio'
        when coalesce(f_dau, false) and not coalesce(f_reward, false) then '4. No OB day sin premio'
        when b.day_ob = a.fecha and not coalesce(f_dau, false) then '5. No DAU (OB day)'
        when b.day_ob <> a.fecha and not coalesce(f_dau, false) then '5. No DAU (No OB day)' -- CREO que esta posibilidad nunca va a salir
        else '4. Desconocido' end as dau_type

FROM economics_mau  a
left join users b on a.user = b.user
ORDER BY 3 desc