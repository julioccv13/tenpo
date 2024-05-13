DROP TABLE IF EXISTS `${project_target}.tmp.sii`;
CREATE TABLE`${project_target}.tmp.sii` AS (
with ruts as(
    SELECT DISTINCT
        a.rut_complete as rut, 
        rand() as random
    FROM `${project_source_1}.identity.ruts` a
        join `${project_source_2}.users.users_allservices` b USING (tributary_identifier) 
    WHERE rut_complete NOT IN (SELECT DISTINCT rut FROM `${project_source_3}.sii.stc_registro_consultas` )
)
select
rut
from ruts 
order by random
limit 100
);


