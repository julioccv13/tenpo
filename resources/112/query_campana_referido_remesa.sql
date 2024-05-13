DROP TABLE IF EXISTS `tenpo-bi.tmp.campana_referidos_remesa_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.campana_referidos_remesa_{{ds_nodash}}` AS 
(

WITH 
nombre as 
(
    select 
    id as user,
    case 
        when lower(first_name) = 'maria jose' then INITCAP(first_name) 
        when lower(first_name) = 'maria fernanda' then INITCAP(first_name) 
        when lower(first_name) = 'maria paz' then INITCAP(first_name) 
        when lower(first_name) = 'maria angelica' then INITCAP(first_name) 
        when lower(first_name) = 'maria ignacia' then INITCAP(first_name) 
        when lower(first_name) = 'maria isabel' then INITCAP(first_name) 
        when lower(first_name) = 'maria teresa' then INITCAP(first_name)
        when lower(first_name) = 'maria jesus' then INITCAP(first_name) 
        when lower(first_name) = 'maria elena' then INITCAP(first_name) 
        when lower(first_name) = 'maria eugenia' then INITCAP(first_name) 
        when lower(first_name) = 'maria francisca' then INITCAP(first_name) 
        when lower(first_name) = 'maria magdalena' then INITCAP(first_name)  
        when lower(first_name) = 'maria belen' then INITCAP(first_name) 
        when lower(first_name) = 'maria del carmen' then INITCAP(first_name) 
        when lower(first_name) = 'luz maria' then INITCAP(first_name) 
        when lower(first_name) = 'juan carlos' then INITCAP(first_name) 
        when lower(first_name) = 'juan pablo' then INITCAP(first_name) 
        when lower(first_name) = 'juan ignacio' then INITCAP(first_name)
        when lower(first_name) = 'juan francisco' then INITCAP(first_name)
        when lower(first_name) = 'juan jose' then INITCAP(first_name)
        when lower(first_name) = 'juan manuel' then INITCAP(first_name)
        when lower(first_name) = 'juan antonio' then INITCAP(first_name)
        when lower(first_name) = 'juan andres' then INITCAP(first_name)
        when lower(first_name) = 'juan luis' then INITCAP(first_name)
        when lower(first_name) = 'juan eduardo' then INITCAP(first_name)
        when lower(first_name) = 'juan alberto' then INITCAP(first_name)
        when lower(first_name) = 'juan guillermo' then INITCAP(first_name)
        when lower(first_name) = 'juan enrique' then INITCAP(first_name)
        when lower(first_name) = 'juan gabriel' then INITCAP(first_name)
        when lower(first_name) = 'juan ramon' then INITCAP(first_name)
        else INITCAP(split(first_name ," ")[offset(0)]) end as primer_nombre
    from `tenpo-airflow-prod.users.users`     
),
users as (
  SELECT
  id,
  rut,
  CONCAT(first_name,' ', last_name)  as nombre
  from
`tenpo-airflow-prod.users.users`
),
users_ob as (
SELECT 
  DISTINCT id AS user, 
  date(ob_completed_at) as fecha_ob
FROM `tenpo-bi-prod.users.users_tenpo` 
  where status_onboarding = 'completo'
),
remesa as (
SELECT 
distinct user, 
fecha, 
trx_id,
trx_timestamp, 
monto
FROM `tenpo-bi-prod.economics.economics`
WHERE linea in ('crossborder')
),
pivot as (
SELECT distinct
 a.fecha,
 a.referrer as padre,
 padre.rut as rut_refiere,
 a.user as hijo,
 hijo.rut as rut_referido,
  hijo.nombre as fullname_referido,
cc.primer_nombre as nombre_referido,
 case when c.user is not null then 1 else 0 end as marca_ob,
 case when d.user is not null and c.fecha_ob >= d.fecha AND DATE_DIFF(c.fecha_ob, d.fecha, DAY) <= 30 then 1 else 0 end as marca_remesa,
 d.trx_id,
 d.trx_timestamp,
 d.monto
FROM `tenpo-bi-prod.payment_loyalty.referrals_iyg` a
LEFT join users padre on padre.id = a.referrer
LEFT join users hijo on hijo.id = a.user
left join users_ob c on c.user = hijo.id
left join remesa d on d.user = hijo.id
left join nombre cc on hijo.id = cc.user
),
logica as (
select 
fecha,
padre,
hijo,
trx_id,
trx_timestamp,
nombre_referido as nombre_referido_remesa,
CASE 
  WHEN marca_ob = 1 and marca_remesa = 1 then 'amigo tenpista e hizo remesa'
  WHEN marca_ob = 1 and marca_remesa = 0 then 'amigo tenpista no ha hecho remesa'
  WHEN marca_ob = 0 and marca_remesa = 0 then 'amigo no es tenpista y no ha hecho remesa'
  WHEN marca_ob = 0 then 'Amigo no tenpista'
END campana_referido_remesa_status,
row_number()over (partition by hijo order by trx_timestamp asc, monto desc) as rnk
from pivot 
where
DATE(trx_timestamp,'America/Santiago') >= '2023-10-02'
and padre in (select user from `tenpo-sandbox.crm.audiencia_extranjeros_CLremesa_04102023`) and hijo in (select user from `tenpo-sandbox.crm.audiencia_extranjeros_CLremesa_04102023`)
),
target as (
select 
padre as identity, 
nombre_referido_remesa, 
campana_referido_remesa_status 
from logica
)
SELECT 
  DISTINCT a.id AS identity, 
  case when b.identity is not null then nombre_referido_remesa else 'N/A' end as nombre_referido_capana_remesa,
  case when b.identity is not null then campana_referido_remesa_status else 'N/A' end as status_campana_referido_remesa
FROM `tenpo-bi-prod.users.users_tenpo` a
LEFT JOIN target b on a.id = b.identity
  where a.status_onboarding = 'completo'

)
