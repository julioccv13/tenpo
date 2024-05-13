DROP TABLE IF EXISTS `tenpo-bi.tmp.numero_cof_mes_presente_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.numero_cof_mes_presente_{{ds_nodash}}` AS 
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
    COUNT(DISTINCT comercio) as subscription_count
  from `tenpo-bi-prod.economics.economics`
  where nombre = 'Suscripcion'
  AND fecha >= DATE_TRUNC(CURRENT_DATE(), MONTH)
  AND fecha < DATE_ADD(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL -1 DAY)
  group by user, month_start
), suscription_pivot as(
select user,
month_start,
subscription_count,
from UserSubscriptionActivity
order by month_start desc)

select distinct a.user as identity, 
case 
when b.user is not null then CAST(b.subscription_count AS STRING) 
when b.USER IS NULL THEN 'N/A'
else 'N/A' end as numero_cof_mes_presente
from user_ob a 
left join suscription_pivot b on a.user = b.user



)
