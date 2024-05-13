DROP TABLE IF EXISTS `${project_target}.tmp.Tiene_Remesa_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Tiene_Remesa_{{ds_nodash}}` AS (
with Tabla_Final as (select a.id as identity, case when b.user is not null then 1 else 0 end M_Crossborder from `${project_source_2}.users.users` a left join (select distinct user from `${project_source_1}.economics.economics`
                                                         where linea = 'crossborder') b
on a.id = b.user
where state = 4)
SELECT
*
from Tabla_Final
);