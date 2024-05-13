DROP TABLE IF EXISTS `${project_target}.tmp.entrega_info_contacto_p2p_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.entrega_info_contacto_p2p_{{ds_nodash}}` AS (

select 
  distinct profile.identity as identity, 
  'entrega informacion' as entrega_info_contacto_p2p 
from `tenpo-bi-prod.external.clevertap_raw`
  where 
  lower(eventname) like ('información de contactos%')
  and fecha = DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
  and profile.identity not like '%,%'
  and profile.identity is not null

)