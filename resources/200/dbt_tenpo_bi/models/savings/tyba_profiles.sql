{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

with tyba_profiles as (
  select distinct
      id,
      user_id,
      min(created_at) over (partition by user_id) as suggested_risk_profile_created_at,
      max(created_at) over (partition by user_id) as lastest_chosen_risk_profile_created_at,
      count(1) over (partition by user_id) as n_profiles_generated,
      array_to_string(array_agg(chosen_risk_profile) over (partition by user_id order by created_at),",") as arr_chosen_risk_profiles,
      case 
        when suggested_risk_profile="ajedrecista" then concat('1. ',suggested_risk_profile)
        when suggested_risk_profile="ciclista" then concat('2. ',suggested_risk_profile)
        when suggested_risk_profile="surfista" then concat('3. ',suggested_risk_profile)
        when suggested_risk_profile="clavadista" then concat('4. ',suggested_risk_profile)
        when suggested_risk_profile="paracaidista" then concat('5. ',suggested_risk_profile)
      end as suggested_risk_profile,
      case 
        when chosen_risk_profile="ajedrecista" then concat('1. ',chosen_risk_profile)
        when chosen_risk_profile="ciclista" then concat('2. ',chosen_risk_profile)
        when chosen_risk_profile="surfista" then concat('3. ',chosen_risk_profile)
        when chosen_risk_profile="clavadista" then concat('4. ',chosen_risk_profile)
        when chosen_risk_profile="paracaidista" then concat('5. ',chosen_risk_profile)
      end as chosen_risk_profile,
  from (select distinct * from {{source('onboarding_savings','user_risk_profile')}})
  where true
  qualify row_number() over (partition by user_id order by created_at desc) = 1
  ),
  users_tyba_trx as (
    select distinct 
      user
    from {{ref('economics')}} where linea like '%tyba%'
  ),
  tyba_profiles_sin_act as (
    select 
      p.*,
      u.user is not null as with_investment
    from tyba_profiles p
      left join users_tyba_trx u on (p.user_id=u.user)
  ),
  saldos as (
    SELECT distinct
      user as user_id,
      MAX(if(b.fecha = date(p.suggested_risk_profile_created_at), saldo_dia, null)) over (partition by user) as first_balance,
      LAST_VALUE(saldo_dia) over (partition by user order by fecha rows between unbounded preceding and unbounded following) as last_balance,
    FROM {{ref('daily_balance')}} b
      join tyba_profiles_sin_act p on (b.user =p.user_id) 
  )
SELECT distinct
  *
FROM saldos s
  join tyba_profiles_sin_act p using (user_id)