DROP TABLE IF EXISTS `${project_target}.tmp.score_ultron_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.score_ultron_{{ds_nodash}}` AS (

--with 
--tabla_pre as 
--(
  SELECT * FROM `${project_source_3}.crm.property_score_ultron`
/*
  select 
    user_id as identity,
    Tipo_incentivo,
    producto,
    max(score) as score
  from `tenpo-datalake-sandbox.jarvis.Proyecto_Personalizacion_Tabla_Ultron`
  group by 1,2,3
)
,tabla as 
(
  select identity,
  case when producto = 'investment' and Tipo_incentivo = 'Con Hook' then score else null end as score_ultron_inversiones_conhook,
  case when producto = 'investment' and Tipo_incentivo = 'Sin Hook' then score else null end as score_ultron_inversiones_sinhook,  
  case when producto = 'investment' and Tipo_incentivo = 'Sorteo' then score else null end as score_ultron_inversiones_sorteo,  
  case when producto = 'mastercard' and Tipo_incentivo = 'Con Hook' then score else null end as score_ultron_mastercard_conhook,  
  case when producto = 'mastercard' and Tipo_incentivo = 'Sin Hook' then score else null end as score_ultron_mastercard_sinhook,  
  case when producto = 'p2p' and Tipo_incentivo = 'Con Hook' then score else null end as score_ultron_p2p_conhook,  
  case when producto = 'p2p' and Tipo_incentivo = 'Sin Hook' then score else null end as score_ultron_p2p_sinhook,  
  case when producto = 'paypal' and Tipo_incentivo = 'Con Hook' then score else null end as score_ultron_paypal_conhook,  
  case when producto = 'paypal' and Tipo_incentivo = 'Sin Hook' then score else null end as score_ultron_paypal_sinhook,  
  case when producto = 'top_ups' and Tipo_incentivo = 'Con Hook' then score else null end as score_ultron_topups_conhook,  
  case when producto = 'top_ups' and Tipo_incentivo = 'Sin Hook' then score else null end as score_ultron_topups_sinhook,  
  case when producto = 'utility_payments' and Tipo_incentivo = 'Con Hook' then score else null end as score_ultron_pdc_conhook,  
  case when producto = 'utility_payments' and Tipo_incentivo = 'Sin Hook' then score else null end as score_ultron_pdc_sinhook,  
  case when producto = 'utility_payments' and Tipo_incentivo = 'Sorteo' then score else null end as score_ultron_pdc_sorteo
  from tabla_pre  
)
select identity
  , max(score_ultron_inversiones_conhook) as score_ultron_inversiones_conhook 
  , max(score_ultron_inversiones_sinhook) as score_ultron_inversiones_sinhook 
  , max(score_ultron_inversiones_sorteo) as score_ultron_inversiones_sorteo 
  , max(score_ultron_mastercard_conhook) as score_ultron_mastercard_conhook 
  , max(score_ultron_mastercard_sinhook) as score_ultron_mastercard_sinhook 
  , max(score_ultron_p2p_conhook) as score_ultron_p2p_conhook 
  , max(score_ultron_p2p_sinhook) as score_ultron_p2p_sinhook 
  , max(score_ultron_paypal_conhook) as score_ultron_paypal_conhook 
  , max(score_ultron_paypal_sinhook) as score_ultron_paypal_sinhook 
  , max(score_ultron_topups_conhook) as score_ultron_topups_conhook 
  , max(score_ultron_topups_sinhook) as score_ultron_topups_sinhook 
  , max(score_ultron_pdc_conhook) as score_ultron_pdc_conhook 
  , max(score_ultron_pdc_sinhook) as score_ultron_pdc_sinhook 
  , max(score_ultron_pdc_sorteo) as score_ultron_pdc_sorteo 
from tabla
group by 1
*/
);

