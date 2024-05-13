DROP TABLE IF EXISTS `tenpo-bi.tmp.plan_usuario_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.plan_usuario_{{ds_nodash}}` AS 
(
  
select distinct id as identity, 
  case 
    when plan = 0 then 'Tenpo'
    when plan = 1 then 'Teenpo'
    when plan = 2 then 'Teenpo menor 14'
    when plan = 3 then 'Teenpo mayor 18' else 'otro/sin categorizar' end as plan_usuario
from `tenpo-bi-prod.users.users_tenpo`

)
