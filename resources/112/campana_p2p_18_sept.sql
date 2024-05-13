DROP TABLE IF EXISTS `tenpo-bi.tmp.campana_p2p_18_sept_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.campana_p2p_18_sept_{{ds_nodash}}` AS (

with 
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
)
,
envio as
(
  select a.requester_id as envia, b.primer_nombre as nombre_envia, a.destination_id as recibe, c.primer_nombre as nombre_recibe, date(a.created_at) as fecha_envio
  from `tenpo-airflow-prod.payment_p2p.p2p_request` a
    left join nombre b on a.requester_id = b.user 
    left join nombre c on a.destination_id = c.user
  where date(a.created_at) between '2023-08-01' and '2023-08-31'
  and a.type = 'PAYMENT' and cast(a.amount as int) = 18
) 
,envio_recibo as 
(
select distinct
  a.fecha_envio as fecha_envio_ida,
  a.envia as envia_ida, 
  a.nombre_envia as nombre_envia_ida,
  a.recibe as recibe_ida,
  a.nombre_recibe as nombre_recibe_ida,
  b.fecha_envio as fecha_envio_vuelta,
  b.envia as envia_vuelta,
  b.nombre_envia as nombre_envia_vuelta,
  b.recibe as recibe_vuelta,
  b.nombre_recibe as nombre_recibe_vuelta,
 case
    when b.envia is null then 'solo envia' 
    when a.envia is null then 'solo recibe' else 'envia y recibe' end as status_sorteo_p2p_18
from envio a
  full join envio b on (a.envia = b.recibe and a.recibe = b.envia)
)
,tabla_pre as 
(
  select 
    case 
      when status_sorteo_p2p_18 = 'solo recibe' then recibe_vuelta
      when status_sorteo_p2p_18 = 'solo envia' then envia_ida
      when status_sorteo_p2p_18 = 'envia y recibe' then envia_ida else 'otro' end as user,
    status_sorteo_p2p_18 as campana_sorteo_p2p_18_status,
    case 
      when status_sorteo_p2p_18 = 'solo recibe' then nombre_envia_vuelta
      when status_sorteo_p2p_18 = 'solo envia' then nombre_recibe_ida
      when status_sorteo_p2p_18 = 'envia y recibe' then nombre_recibe_ida else 'otro' end as campana_sorteo_p2p_18_nombre_amigo,
  from envio_recibo
)
,tabla as 
(
  select 
    user as identity,
    campana_sorteo_p2p_18_status,
    campana_sorteo_p2p_18_nombre_amigo,
    ROW_NUMBER() OVER (PARTITION BY user order by campana_sorteo_p2p_18_status asc) as rank
  from tabla_pre
)
,tabla_final as 
(
  select identity, campana_sorteo_p2p_18_status, campana_sorteo_p2p_18_nombre_amigo
  from tabla 
  where rank = 1 
)
select
      a.id as identity
      ,ifnull(b.campana_sorteo_p2p_18_status,'sin accion campana') as campana_sorteo_p2p_18_status
      ,ifnull(b.campana_sorteo_p2p_18_nombre_amigo,'sin accion campana') as campana_sorteo_p2p_18_nombre_amigo
from `tenpo-bi-prod.users.users_tenpo` a
  left join tabla_final b on a.id = b.identity
where a.status_onboarding = 'completo'
)