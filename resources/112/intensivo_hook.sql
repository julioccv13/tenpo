DROP TABLE IF EXISTS `${project_target}.tmp.intensivo_hook_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.intensivo_hook_{{ds_nodash}}` AS (
with
REWARDS as
(
  select
  T1.user,
  sum(monto) as reward_total
  from `${project_source_1}.economics.economics` T1
  where
    T1.fecha between date_sub(current_date(), interval 90 day) and date_sub(current_date(), interval 1 day)
    and lower(linea) ='reward'
    and nombre not like '%Home%'
    and lower(nombre) not like '%devoluc%'
  group by 1
),
ECONOMICS as
(
  select
  T1.user,
  sum(monto) as gpv_total
  from `${project_source_1}.economics.economics` T1
  where
    T1.linea in('mastercard','mastercard_physical','utility_payments','top_ups','paypal','paypal_abonos','crossborder')
    and T1.fecha between date_sub(current_date(), interval 90 day) and date_sub(current_date(), interval 1 day)
    and lower(linea) not in ('saldo','reward')
    and linea not like '%PFM%'
    and nombre not like '%Home%'
    and lower(nombre) not like '%devoluc%'
  group by 1
),
users as (
  select distinct user
  from ECONOMICS

  union distinct

  select distinct user
  from REWARDS
),
ratio_users as (
  select
  a.user,
  reward_total,
  gpv_total,
  safe_divide(reward_total,gpv_total) as ratio_total,
  from users a
  left join ECONOMICS b on a.user=b.user
  left join REWARDS c on a.user=c.user
),
percentiles as (
  select
    percentiles[offset(10)] as p10,
    percentiles[offset(20)] as p20,
    percentiles[offset(30)] as p30,
    percentiles[offset(40)] as p40,
    percentiles[offset(50)] as p50,
    percentiles[offset(60)] as p60,
    percentiles[offset(70)] as p70,
    percentiles[offset(80)] as p80,
    percentiles[offset(90)] as p90,
    percentiles[offset(100)] as p100,
  from (
    select approx_quantiles(ratio_total, 100) percentiles
    from ratio_users
    
  )
),
ind_hook_final as (
  select a.*,
  case when a.ratio_total>p50 then 1 else 0 end as ind_intensivo_hook
  from ratio_users a
  left join percentiles on 1=1
)
, tabla_final as 
(
select distinct
user as identity,
'intensivo' as ind_intensivo_hook
from ind_hook_final
where ind_intensivo_hook=1
)
select distinct
      T1.id as identity,
      IFNULL(T2.ind_intensivo_hook,'no intensivo') as intensivo_hook
from `${project_source_1}.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.identity
where status_onboarding = 'completo'
)
