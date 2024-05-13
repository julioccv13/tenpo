DROP TABLE IF EXISTS `tenpo-bi.tmp.contacto_ob_ayer_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.contacto_ob_ayer_{{ds_nodash}}` AS (
with nombre as 
(
    select 
    id,
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
,tabla_final as 
(
  select distinct user_id, contact_id, date(b.ob_completed_at) as fecha_ob_contacto, c.primer_nombre as nombre_contacto
  from `tenpo-airflow-prod.cca_tef_contacts.user_contact` a 
    left join `tenpo-airflow-prod.users.users` b on (a.contact_id = b.id or right(a.phone_number,8) = right(b.phone,8))
    left join nombre c on a.contact_id = c.id
  where contact_id is not null and date(b.ob_completed_at) = date_sub(current_date(), interval 1 day) and c.primer_nombre is not null
)
select
      a.id as identity
      ,max(ifnull(b.nombre_contacto,'ningun contacto hizo ob ayer')) as contacto_ob_ayer
from `tenpo-bi-prod.users.users_tenpo` a
  left join tabla_final b on a.id = b.user_id
where a.status_onboarding = 'completo'
group by 1
);