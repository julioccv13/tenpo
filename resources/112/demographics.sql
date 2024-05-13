DROP TABLE IF EXISTS `${project_target}.tmp.demographics_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.demographics_{{ds_nodash}}` AS (
select 
        id_usuario as identity,
        edad
from `${project_source_1}.users.demographics`
);
