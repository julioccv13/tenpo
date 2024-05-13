DROP TABLE IF EXISTS `tenpo-bi.tmp.cruce_nov_listado_productos_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.cruce_nov_listado_productos_{{ds_nodash}}` AS (

with target as (
select distinct user as identity,
listado_productos as cruce_nov_listado_productos
from `tenpo-sandbox.crm.avance_campana_cruce_noviembre`
),
users_ob as (
  select distinct a.id as identity
  from `tenpo-bi-prod.users.users_tenpo` a
  left join `tenpo-bi-prod.users.demographics`  b on a.id = b.id_usuario
  where a.status_onboarding = 'completo' and b.estado_churn in ('Cliente Activo','Inactivo mau','Inactivo no mau') and a.state = 4
)
select a.identity,
  CASE
  WHEN b.identity is not null then b.cruce_nov_listado_productos else 'N/A' end as cruce_nov_listado_productos
from users_ob a 
left join target b on a.identity = b.identity

)