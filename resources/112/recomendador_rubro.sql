DROP TABLE IF EXISTS `tenpo-bi.tmp.modelo_recomendador_rubro_top_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.modelo_recomendador_rubro_top_{{ds_nodash}}` AS (
with data_recomendador as 
(
  SELECT user, rubro, corr, ROW_NUMBER() OVER (PARTITION BY user order by corr desc) as rank
  FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador` 
  WHERE fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador` )
  order by user, corr desc  
)
,rubro1 as 
(select user, rubro as recomendacion_rubro_1
from data_recomendador
where rank = 1)
,rubro2 as 
(select user, rubro as recomendacion_rubro_2
from data_recomendador
where rank = 2)
,rubro3 as 
(select user, rubro as recomendacion_rubro_3
from data_recomendador
where rank = 3)
select
      a.id as identity
      ,ifnull(b.recomendacion_rubro_1,'sin recomendacion') as modelo_recomendador_rubro_top1
      ,ifnull(c.recomendacion_rubro_2,'sin recomendacion') as modelo_recomendador_rubro_top2
      ,ifnull(d.recomendacion_rubro_3,'sin recomendacion') as modelo_recomendador_rubro_top3
from `tenpo-bi-prod.users.users_tenpo` a
  left join rubro1 b on a.id = b.user
  left join rubro2 c on a.id = c.user
  left join rubro3 d on a.id = d.user
where a.status_onboarding = 'completo'
)
