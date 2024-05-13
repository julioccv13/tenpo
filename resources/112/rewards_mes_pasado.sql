DROP TABLE IF EXISTS `${project_target}.tmp.reward_mes_pasado_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.reward_mes_pasado_{{ds_nodash}}` AS (
with tabla_final as 
(
    select user as identity, sum(monto) as reward_mes_pasado
    from `${project_source_1}.economics.economics`
    where fecha >= '2023-01-01' and linea = 'reward'
    and (extract(year from fecha)*100+extract(month from fecha)) = (extract(year from date_add(current_date(), interval -1 month))*100+extract(month from date_add(current_date(), interval -1 month)))
    group by 1
)
select distinct
      T1.id as identity,
      IFNULL(T2.reward_mes_pasado,0) as reward_mes_pasado
from `${project_source_1}.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.identity
where status_onboarding = 'completo'
)