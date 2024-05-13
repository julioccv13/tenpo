DROP TABLE IF EXISTS `tenpo-bi.tmp.pais_destino_remesa_mas_frecuente{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.pais_destino_remesa_mas_frecuente{{ds_nodash}}` AS 
(


  WITH CountByUser AS (
  SELECT 
    user,
    nombre AS nombre_pais,
    COUNT(*) AS cnt
  FROM `tenpo-bi-prod.economics.economics`
  WHERE linea = 'crossborder'
  GROUP BY user, nombre
),

RankedByUser AS (
  SELECT 
    user as identity,
    nombre_pais,
    cnt,
    ROW_NUMBER() OVER(PARTITION BY user ORDER BY cnt DESC) as rn
  FROM CountByUser
),
users_ob AS (
  SELECT DISTINCT id AS user
  FROM `tenpo-bi-prod.users.users_tenpo`
  WHERE status_onboarding = 'completo'
),
final as (
SELECT 
  distinct identity,
  case when nombre_pais is null then 'No registra destino'
  else nombre_pais end as pais_destino_remesa_mas_frecuente
FROM RankedByUser
WHERE rn = 1 )

select distinct a.user as identity, case when b.identity is null then 'N/A' when b.identity is not null then b.pais_destino_remesa_mas_frecuente else 'N/A' end as pais_destino_remesa_mas_frecuente
from users_ob a 
left join final b on a.user = b.identity


)
