DROP TABLE IF EXISTS `tenpo-bi.tmp.permanencia_usuario_inversiones_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.permanencia_usuario_inversiones_{{ds_nodash}}` AS 
(

with
variables as (
    SELECT
        LAST_DAY(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)) as fecha_cohort,
        ARRAY<STRING>['investment_tyba'] as products
),
cohort_fecha AS (
    SELECT id AS user,
           cohort,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY cohort ASC) AS rnk
    FROM `tenpo-bi-prod.campaign_management.funnel_users_history`
    CROSS JOIN variables
    WHERE created = variables.fecha_cohort
),

cohort_min_fecha AS (
    SELECT MIN(created) AS min_fecha
    FROM `tenpo-bi-prod.campaign_management.funnel_users_history`
),

cohort_last_update AS (
    SELECT id AS user,
           cohort,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY cohort ASC) AS rnk
    FROM `tenpo-bi-prod.campaign_management.funnel_users_history`,
         cohort_min_fecha
    WHERE created = cohort_min_fecha.min_fecha
),

users_ob AS (
    SELECT DISTINCT id AS user,
           EXTRACT(YEAR FROM fecha_creacion) * 100 + EXTRACT(MONTH FROM fecha_creacion) AS mes_ob,
    FROM `tenpo-bi-prod.users.users_tenpo`
    WHERE status_onboarding = 'completo'
),

users_maus AS (
    SELECT DISTINCT
        EXTRACT(YEAR FROM fecha) * 100 + EXTRACT(MONTH FROM fecha) AS mes,
        user
    FROM `tenpo-bi-prod.economics.economics` a,
         variables
    WHERE
        linea IN UNNEST(variables.products)
        AND linea NOT LIKE '%PFM%'
        AND nombre NOT LIKE '%Home%'
        AND linea != 'Saldo'
        AND linea != 'saldo'
        AND LOWER(nombre) NOT LIKE '%devoluc%'
        AND linea <> 'reward'
        AND fecha BETWEEN DATE_SUB(variables.fecha_cohort, INTERVAL 8 MONTH) AND variables.fecha_cohort
),
users_base as (
  select distinct user
  from (
    select mes, user from users_maus
  )
),
meses_base as (
  select distinct mes
  from (
    select mes, user from users_maus
  )
),
apertura_general as (
  select user, mes
  from users_base
  left join meses_base on 1=1
),
users_maus_general as (
  SELECT distinct
    a.mes,
    a.user,
    case when b.user is not null then 1 else 0 end as ind_mau
  FROM apertura_general a
  left join users_maus b on a.user=b.user and a.mes=b.mes
),
users_maus_lag_v1 as (
  select *,
  LAG(ind_mau,1) OVER (PARTITION BY user ORDER BY mes ASC) AS ind_mau_m1,
  LAG(ind_mau,2) OVER (PARTITION BY user ORDER BY mes ASC) AS ind_mau_m2,
  LAG(ind_mau,3) OVER (PARTITION BY user ORDER BY mes ASC) AS ind_mau_m3,
  LAG(ind_mau,4) OVER (PARTITION BY user ORDER BY mes ASC) AS ind_mau_m4,
  LAG(ind_mau,5) OVER (PARTITION BY user ORDER BY mes ASC) AS ind_mau_m5,
  LAG(ind_mau,6) OVER (PARTITION BY user ORDER BY mes ASC) AS ind_mau_m6,
  from users_maus_general
),
users_maus_lag_v1_2 as (
  select a.*,
  case
    when ind_mau=1 and ind_mau_m6=1 and ind_mau_m5=1 and ind_mau_m4=1 and ind_mau_m3=1 and ind_mau_m2=1 and ind_mau_m1=1 then 'mau 6M+'
    when ind_mau=1 and ind_mau_m5=1 and ind_mau_m4=1 and ind_mau_m3=1 and ind_mau_m2=1 and ind_mau_m1=1 then 'mau 5M'
    when ind_mau=1 and ind_mau_m4=1 and ind_mau_m3=1 and ind_mau_m2=1 and ind_mau_m1=1 then 'mau 4M'
    when ind_mau=1 and ind_mau_m3=1 and ind_mau_m2=1 and ind_mau_m1=1 then 'mau 3M'
    when ind_mau=1 and ind_mau_m2=1 and ind_mau_m1=1 then 'mau 2M'
    when ind_mau=1 and ind_mau_m1=1 then 'mau 1M'
    when ind_mau=1 and b.user is not null then 'mau mes_ob'
    when ind_mau=1 then 'mau esp'
    else null end as permanencia
  from users_maus_lag_v1 a
  left join users_ob b on a.user=b.user and a.mes=b.mes_ob
),
permanencia AS (
    SELECT
    a.user,
    a.mes,
    b.permanencia,
    FROM users_maus_general a
    CROSS JOIN variables
    LEFT JOIN users_maus_lag_v1_2 b ON a.user=b.user AND a.mes=b.mes
    WHERE a.mes = EXTRACT(YEAR FROM variables.fecha_cohort) * 100 + EXTRACT(MONTH FROM variables.fecha_cohort)
),
usuarios as (
  select distinct user from cohort_fecha
  union distinct
  select distinct user from cohort_last_update
  union distinct
  select distinct user from permanencia
),
tabla_final as (
  select
  a.user,
  coalesce(b.permanencia, c.cohort, d.cohort) as cohort
  from usuarios a
  left join permanencia b on a.user=b.user
  left join cohort_fecha c on a.user=c.user and c.rnk=1
  left join cohort_last_update d on a.user=d.user and d.rnk=1
),
tabla_final_agrup as (
  select
  user,
  case
    when cohort='ob_incompleto'      then 'NO_MAU'
    when cohort='no_ready'           then 'NO_MAU'
    when cohort='inactivos'          then 'NO_MAU'
    when cohort='activos_antes_2022' then 'NO_MAU'
    when cohort='activos_2022'       then 'NO_MAU'
    when cohort='activos_mes_anterior' then 'NO_MAU'
    when cohort='activos_ultimo_mes' then 'NO_MAU'
    when cohort='mau mes_ob'         then 'MAU_MES_OB'
    when cohort='mau esp'            then 'MAU_ESPORADICO'
    when cohort='mau 1M'             then 'MAU_1M'
    when cohort='mau 2M'             then 'MAU_2M'
    when cohort='mau 3M'             then 'MAU_3M'
    when cohort='mau 4M'             then 'MAU_4M'
    when cohort='mau 5M'             then 'MAU_5M'
    when cohort='mau 6M+'            then 'MAU_6M+'
    else null end as cohort
  from tabla_final
)
select distinct user as identity, cohort as permanencia_usuario_inversiones_
from tabla_final_agrup

)
