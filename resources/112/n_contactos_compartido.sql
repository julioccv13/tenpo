DROP TABLE IF EXISTS `${project_target}.tmp.n_contactos_compartido_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.n_contactos_compartido_{{ds_nodash}}` AS (
with tabla_final as 
(
  WITH
  users AS (
    SELECT DISTINCT id AS user, right(phone,8) AS user_phone, first_name
    FROM `tenpo-airflow-prod.users.users`
  ),
  contactos_pre AS (
    SELECT DISTINCT user_id AS user, right(phone_number,8) AS phone
    FROM `tenpo-airflow-prod.cca_tef_contacts.user_contact`
  ),
  contactos as
  (
    select distinct a.*, b.user as user_contacto, b.first_name as nombre_contacto
    from contactos_pre a
      left join users b on a.phone = b.user_phone
  )
  select a.user, count(distinct b.user_contacto) as n_contactos_tenpo_compartido
  from users a
    left join contactos b on (a.user = b.user)
  group by 1
)
select distinct
      T1.id as identity,
      IFNULL(cast(T2.n_contactos_tenpo_compartido as string),'0 o s/i') as n_contactos_tenpo_compartido
from `tenpo-bi-prod.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.user
where status_onboarding = 'completo'
)



