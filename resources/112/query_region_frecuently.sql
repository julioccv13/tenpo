DROP TABLE IF EXISTS `tenpo-bi.tmp.region_frecuently_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.region_frecuently_{{ds_nodash}}` AS 
(

with rank as (
  SELECT 
  user_id, 
  region,
  count(1) as conteo,
  rank() over (partition by user_id order by count(1) desc) as rank_orden
  FROM `tenpo-datalake-prod.georeferencing.data_georeferencing`
  where region is not null
  group by 1,2
),
region as (
  select user_id,
  region
  from rank 
  where rank_orden = 1
),
users as (
  SELECT DISTINCT id AS user
  FROM `tenpo-bi-prod.users.users_tenpo` 
  WHERE status_onboarding = 'completo'
)
select 
distinct a.user as identity,

case 
when b.user_id is not null then b.region
when b.user_id is null then 'N/A'
else 'N/A' end as region_frecuente

from users a 
left join region b on a.user = b.user_id 

)
