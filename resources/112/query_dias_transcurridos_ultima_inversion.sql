DROP TABLE IF EXISTS `tenpo-bi.tmp.dias_transcurridos_ult_inversion{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.dias_transcurridos_ult_inversion{{ds_nodash}}` AS 
(


with users_ob as (
SELECT DISTINCT id AS user
FROM `tenpo-bi-prod.users.users_tenpo` 
WHERE status_onboarding = 'completo'
),
ultim_fecha as (
  select user, max(fecha) as ult_fecha
from `tenpo-bi-prod.economics.economics`
where linea in ('investment_tyba')
group by 1
),
final as (
  select distinct user, DATE_DIFF(CURRENT_DATE, ult_fecha, DAY) AS dias_transcurridos
  from ultim_fecha
)

select distinct a.user as identity, case when b.user is not null then CAST(b.dias_transcurridos AS STRING) else 'N/A' end as dias_transcurridos_ult_inversion
from users_ob a 
left join final b on a.user = b.user 

)
