DROP TABLE IF EXISTS `tenpo-bi.tmp.status_usuario_cof_mes_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_usuario_cof_mes_{{ds_nodash}}` AS 
(

with user_ob as (
select distinct id as user 
from `tenpo-bi-prod.users.users_tenpo`
where status_onboarding = 'completo'
),
UserSubscriptionActivity as (
  select
    user,
    DATE_TRUNC(DATE_TRUNC(fecha, MONTH), MONTH) as month_start,
    COUNT(DISTINCT trx_id) as subscription_count
  from `tenpo-bi-prod.economics.economics`
  where nombre = 'Suscripcion'
  AND fecha >= DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 3 MONTH, MONTH)
  AND fecha < DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH), MONTH)
  group by user, month_start
),
suscriptions_last as (
select user,
month_start,
subscription_count,
lag(subscription_count,1) over (PARTITION BY user ORDER BY month_start) as prev_subscription_count,
lag(subscription_count, 2) over (PARTITION BY user ORDER BY month_start) as prev_prev_subscription_count,
ROW_NUMBER() OVER (PARTITION BY user ORDER BY month_start DESC) AS row_num
from UserSubscriptionActivity
),
tabla_final as (
select distinct
  user,
  case
    when ifnull(subscription_count,0) >= ifnull(prev_subscription_count,0) then 'Activo'
    when ifnull(prev_subscription_count,0) >= ifnull(prev_prev_subscription_count,0) then 'Activo_hace_1mes'
    when ifnull(subscription_count,0)<ifnull(prev_prev_subscription_count,0) AND ifnull(prev_subscription_count,0) < ifnull(prev_prev_subscription_count,0) then 'Activo_hace_2meses'
    else 'Inactivo'
  end as estado
from suscriptions_last where row_num = 1
)

select 
a.user as identity,
case 
when b.user is null then 'Nunca_cof'
when b.user is not null then b.estado
else 'Nunca_cof'
end status_usuario_cof_mes
from user_ob a
left join tabla_final b on a.user = b.user

)
