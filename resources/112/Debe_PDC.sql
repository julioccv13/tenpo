DROP TABLE IF EXISTS `${project_target}.tmp.Debe_PDC_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Debe_PDC_{{ds_nodash}}` AS (
with debe_pdc as 
(
  SELECT
  distinct user as identity, 1 as Debe_PDC
  FROM
    `${project_source_2}.tenpo_utility_payment.bills`
  WHERE
    DATE(updated) >= DATE_ADD(CURRENT_DATE(),INTERVAL -30 DAY)
    and confirm_transaction_id is null
)
, Tabla_Final as (
SELECT T1.id_usuario as identity, case when T2.Debe_PDC = 1 then 1 else 0 end as Debe_PDC
FROM `${project_source_1}.users.demographics` T1
  left join debe_pdc T2 on T1.id_usuario = T2.identity
)

SELECT
*
from Tabla_Final
);