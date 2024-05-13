DROP TABLE IF EXISTS `tenpo-bi.tmp.tenpista_en_extranjero_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.tenpista_en_extranjero_{{ds_nodash}}` AS 
(

with users as (
  select id as user from `tenpo-bi-prod.users.users_tenpo` where status_onboarding = 'completo'
),
ultimo_geo as (
    select user_id as user,
    FIRST_VALUE(country) OVER (PARTITION BY user_id ORDER BY created_at DESC) AS ultimo_pais
  from `tenpo-datalake-prod.georeferencing.data_georeferencing`
  where country IS NOT NULL
    and created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), interval 10 day)
),
geo as (
select user_id,
    case
    when max(a.country) = min(a.country) and max(a.country) = 'CL' and MAX(b.ultimo_pais) = 'CL' THEN 'Chile'
    when max(a.country) = min(a.country) and max(a.country) != 'CL' and MAX(b.ultimo_pais) != 'CL' THEN 'Extranjero'
    when MAX(b.ultimo_pais) = 'CL' THEN 'Chile'
    when MAX(b.ultimo_pais) != 'CL' and min(b.ultimo_pais) != 'CL' THEN 'Extranjero'
    ELSE 'No registra'
    end as location_status,

    count(*) AS cont,

    RANK() OVER (PARTITION BY a.user_id order by COUNT(*) DESC) AS ranking
  from (
    select user_id, country, created_at
    from `tenpo-datalake-prod.georeferencing.data_georeferencing`
    where country is not null and created_at >= timestamp_sub(current_timestamp(), interval 10 day)
      ) as a 
    left join ultimo_geo b on b.user = a.user_id

  group by user_id, country
)

select distinct a.user as identity, 
case 
  when b.user_id is null then 'No registra' 
  when b.user_id is not null then b.location_status
  else b.location_status
end as tenpista_en_extranjero,
from users a
left join geo b on b.user_id = a.user

)
