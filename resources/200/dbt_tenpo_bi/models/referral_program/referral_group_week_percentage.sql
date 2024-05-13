{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}
SELECT
  *,
  total_grupo_semana/total_grupo porc_grupo_semana
FROM(
    SELECT
      sem_ingreso_beta,
      isoweek_diff,
      total_grupo,
      SUM(cuenta_ob_iyg) total_grupo_semana
    FROM {{ ref('referrals_group_week') }} --`tenpo-bi-prod.referral_program.referrals_group_week` 
    LEFT JOIN (
      SELECT
        sem_ingreso_beta,
        SUM(cuenta_ob_iyg) total_grupo
      FROM {{ ref('referrals_group_week') }} --`tenpo-bi-prod.referral_program.referrals_group_week` 
      GROUP BY 
        1
    ) USING(sem_ingreso_beta)
    GROUP BY 
      1,2,3
)