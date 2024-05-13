DROP TABLE IF EXISTS `tenpo-bi.tmp.recencia_pdc{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.recencia_pdc{{ds_nodash}}` AS 
(

select distinct b.user as identity, 
CASE WHEN a.value IS NOT NULL THEN CAST(a.value AS STRING) ELSE 'NULL' END as recencia_dias_pdc
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'recencia_pdc_d'

)
