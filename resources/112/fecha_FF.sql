DROP TABLE IF EXISTS `${project_target}.tmp.fecha_FF_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.fecha_FF_{{ds_nodash}}` AS (

SELECT
  DISTINCT id as identity,
  CONCAT(EXTRACT(day
    FROM
      DATE_ADD(CURRENT_DATE(),INTERVAL 4 DAY)),'/',EXTRACT(month
    FROM
      DATE_ADD(CURRENT_DATE(),INTERVAL 4 DAY))) fecha_FF
FROM
  `${project_source_1}.users.users_tenpo`

)