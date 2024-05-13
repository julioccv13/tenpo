DROP TABLE IF EXISTS `tenpo-bi.tmp.tutor_activo_teenpo_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.tutor_activo_teenpo_{{ds_nodash}}` AS (
select
      a.id as identity, ifnull(b.tutor_activo_teenpo, 'no') as tutor_activo_teenpo
from `tenpo-bi-prod.users.users_tenpo` a
  left join 
  (
    select distinct authorizer_user_id, 'si' as tutor_activo_teenpo
    from `tenpo-bi-prod.teenpo.teenpo_authorization`
    where status = 'APPROVED'
  ) b on a.id = b.authorizer_user_id
where a.status_onboarding = 'completo'
)
