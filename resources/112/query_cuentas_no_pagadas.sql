DROP TABLE IF EXISTS `tenpo-bi.tmp.cuentas_no_pagadas{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.cuentas_no_pagadas{{ds_nodash}}` AS 
(

WITH cuentas_top AS (
  SELECT 'Telecomunicaciones' AS cuenta
  UNION ALL
  SELECT 'Luz' AS cuenta
  UNION ALL
  SELECT 'Agua' AS cuenta
  UNION ALL
  SELECT 'Gas' AS cuenta
  UNION ALL
  SELECT 'Autopistas' AS cuenta
  UNION ALL
  SELECT 'Financiera' AS cuenta
  UNION ALL
  SELECT 'Retail' AS cuenta
  UNION ALL
  SELECT 'Educación' AS cuenta
),
region as (
select id_usuario as user, sigla_region as region_user
from `tenpo-bi-prod.users.demographics`
),
industrias as (
select distinct nombre as industrias1, user
FROM `tenpo-bi-prod.economics.economics`
where linea in('utility_payments')
and fecha >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
),
array_cuentas as (
select user, industrias1 as industrias_
from industrias
group by user,industrias_
),
cuentas_pagadas as (
select user, string_agg(industrias_,', ') as cuentas_cliente
from array_cuentas
group by user
),
cuentas_no_pagadas as ( 
SELECT cp.user, STRING_AGG(ct.cuenta, ', ') AS cuentas
FROM cuentas_top ct
CROSS JOIN cuentas_pagadas cp
LEFT JOIN UNNEST(SPLIT(cp.cuentas_cliente, ', ')) AS cuenta_usuario ON LOWER(TRIM(cuenta_usuario)) = LOWER(TRIM(ct.cuenta))
left join region r on r.user = cp.user 
WHERE cuenta_usuario IS NULL
GROUP BY cp.user)

select user as identity, cuentas as cadena_cuentas_no_pagadas,

CONCAT(
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(0)],', ',
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(1)],' o ',
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(2)]
) AS cuentas_no_pagadas_first3,
CONCAT(
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(0)],' o ',
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(1)]
) AS cuentas_no_pagadas_first2,
CONCAT(
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(3)],', ',
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(4)],' o ',
         SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(5)]
) AS cuentas_no_pagadas_last6,

 SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(0)] AS no_paga_cuenta1,
  SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(1)] AS no_paga_cuenta2,
  SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(2)] AS no_paga_cuenta3,
  SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(3)] AS no_paga_cuenta4,
  SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(4)] AS no_paga_cuenta5,
  SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(5)] AS no_paga_cuenta6,
  SPLIT(TRIM(cuentas), ',')[SAFE_OFFSET(6)] AS no_paga_cuenta7
from cuentas_no_pagadas
where user is not null and cuentas is not null

)
