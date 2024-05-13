DROP TABLE IF EXISTS `${project_target}.tmp.recency_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.recency_{{ds_nodash}}` AS (

with 
recency_pdc as 
(
    select user, max(fecha) as fecha_ultimo_pdc
    from `${project_source_1}.economics.economics`
    where linea = 'utility_payments'
    group by 1
)
,Tabla_Final as (

select
      a.id as identity, date_diff(current_date(), fecha_ultimo_pdc,  day) as recency_pdc, rand() as numero_aleatorio
from `${project_source_1}.users.users_tenpo` a
    left join recency_pdc b on a.id = b.user
where a.status_onboarding = 'completo'
)

SELECT
*
from Tabla_Final
);
