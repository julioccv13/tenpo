DROP TABLE IF EXISTS `${project_target}.tmp.query_ob_daddy_yankee_15_19_mayo_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.query_ob_daddy_yankee_15_19_mayo_{{ds_nodash}}` AS (
WITH 
TABLA_CLIENTES AS
(
  select distinct
  id as user,
  EXTRACT(DATE from ob_completed_at) as FECHA_OB
  from `${project_source_2}.users.users` 
  where EXTRACT(YEAR FROM EXTRACT(date from ob_completed_at)) >= 2017 
), tabla_final as (

select 
  T1.user as identity,
  case when T1.FECHA_OB >= '2022-05-15' and T1.FECHA_OB <= '2022-05-19' then 1 else 0 end OB_DADDY_YANKEE_15_19_mayo
from TABLA_CLIENTES T1)

SELECT
*
from tabla_final
);