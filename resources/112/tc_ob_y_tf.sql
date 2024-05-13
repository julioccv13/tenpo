DROP TABLE IF EXISTS `tenpo-bi.tmp.tc_ob_y_tf_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.tc_ob_y_tf_{{ds_nodash}}` AS (
    
with ob_tc as 
(
    SELECT distinct user_id as identity, min(status) as tc_status_ob 
    FROM `tenpo-airflow-prod.credits.onboarding` 
    group by 1
)
,tarjeta_fisica_tc as 
(
    SELECT distinct user_id as identity, min(status) as tc_status_tarjeta_fisica
    FROM `tenpo-airflow-prod.credit_card.physical_card_activation` 
    group by 1
)
select distinct
      a.id as identity,
      IFNULL(cast(b.tc_status_ob as string),'no ha iniciado ob tc') as tc_status_ob,
      IFNULL(cast(c.tc_status_tarjeta_fisica as string),'no ha solicitado tc fisica') as tc_status_tarjeta_fisica
from `tenpo-bi-prod.users.users_tenpo` a
  left join ob_tc b on a.id = b.identity
  left join tarjeta_fisica_tc c on a.id = c.identity
where a.status_onboarding = 'completo'

)




