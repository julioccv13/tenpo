{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH users as (
    SELECT DISTINCT
      id as user
      ,DATE_TRUNC(date(ob_completed_At, 'America/Santiago'), MONTH) as mes_ob
    FROM {{ ref('users_tenpo') }} --`tenpo-bi-prod.users.users_tenpo`
  ), economics as (
    SELECT 
      DATE_TRUNC(fecha, MONTH) as mes
      ,user
      ,max(linea = 'reward') as f_reward
      ,max(linea <> 'reward' and nombre not like '%Devol%') as f_mau
    FROM {{ ref('economics') }} --`tenpo-bi-prod.economics.economics` -- USING(user)
    group by 1, 2
  )

SELECT 
    a.user
    ,a.mes
    ,case when b.mes_ob = a.mes and coalesce(f_mau, false) and coalesce(f_reward, false) then '1. OB Mes con premio'
        when b.mes_ob = a.mes and coalesce(f_mau, false) and not coalesce(f_reward, false)  then '2. OB Mes sin premio'
        when coalesce(f_mau, false) and coalesce(f_reward, false) then '3. No OB Mes con premio'
        when coalesce(f_mau, false) and not coalesce(f_reward, false) then '4. No OB Mes sin premio'
        when b.mes_ob = a.mes and not coalesce(f_mau, false) then '5. No MAU (OB Mes)'
        when b.mes_ob <> a.mes and not coalesce(f_mau, false) then '5. No MAU (No OB Mes)'
        else '4. Desconocido' end as mau_type
FROM economics  a
left join users b 
on a.user = b.user and a.mes >= b.mes_ob