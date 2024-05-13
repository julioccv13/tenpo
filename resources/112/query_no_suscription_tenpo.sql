DROP TABLE IF EXISTS `tenpo-bi.tmp.no_suscription_tenpo{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.no_suscription_tenpo{{ds_nodash}}` AS 
(

WITH suscripciones AS (
  SELECT
    user_id,
      CASE
    WHEN REGEXP_CONTAINS(INITCAP(REGEXP_REPLACE(isp_name, '^"|"$', '')), r'Entel') THEN 'Entel'
    WHEN REGEXP_CONTAINS(INITCAP(REGEXP_REPLACE(isp_name, '^"|"$', '')), r'Telefonica') THEN 'Telefonica'
    ELSE INITCAP(REGEXP_REPLACE(isp_name, '^"|"$', ''))
  END AS cuenta
  FROM `tenpo-airflow-prod.identity_provider.seon_structure`
  WHERE isp_name IS NOT NULL
  GROUP BY user_id, cuenta
),
users_ob as (
  SELECT DISTINCT ob.id AS user
  FROM suscripciones u
  LEFT JOIN `tenpo-bi-prod.users.users_tenpo` AS ob ON ob.id = u.user_id
  WHERE ob.status_onboarding = 'completo'
),
economics AS (
  SELECT
    user,
    comercio
  FROM `tenpo-bi-prod.economics.economics`
  WHERE fecha >= DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 MONTH, MONTH)
  AND fecha < DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH), MONTH)
  and nombre = 'Suscripcion' 
)
SELECT s.user_id AS identity, 
  REGEXP_REPLACE(SPLIT(STRING_aGG(s.cuenta,';'),';')[ORDINAL(1)], r'( S\.?A\.?| Spa\.?)', '') AS suscription_no_tenpo_1,
  CASE WHEN ARRAY_LENGTH(SPLIT(STRING_aGG(s.cuenta,';'),';')) >= 2 THEN REGEXP_REPLACE(SPLIT(STRING_aGG(s.cuenta,';'),';')[ORDINAL(2)], r'( S\.?A\.?| Spa\.?)', '') ELSE NULL END AS suscription_no_tenpo_2
from suscripciones s
left join economics e ON LOWER(SPLIT(s.cuenta, ' ')[OFFSET(0)]) = LOWER(SPLIT(e.comercio, ' ')[OFFSET(0)]) AND s.user_id = e.user
WHERE e.user IS NULL and s.user_id is not null
GROUP BY s.user_id

)
