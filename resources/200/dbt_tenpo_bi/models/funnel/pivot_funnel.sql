{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}


WITH data AS 

    (SELECT 
            user,
            DATE_TRUNC(DATE(fecha_ob),month) mes,
            CASE 
                WHEN min_to_ob	< 3 THEN 'menor_3_min'
                WHEN min_to_ob	>=3 AND min_to_ob < 5 THEN 'entre_3_5_min'
                WHEN min_to_ob	>= 5 AND min_to_ob	< 15 THEN 'entre_5_15_min'
                WHEN min_to_ob	>= 15 AND min_to_ob	< 30 THEN 'entre_15_30_min'
                WHEN min_to_ob	>= 30 AND min_to_ob	< 60 THEN 'entre_30_1_hr'
                WHEN min_to_ob	>= 60 AND min_to_ob	< 180 THEN 'entre_1_3_hr'
                WHEN min_to_ob	>= 180 AND min_to_ob	< 720 THEN 'entre_3_12_hrs'
                WHEN min_to_ob	>= 720 AND min_to_ob	< 1440 THEN 'entre_12_24_hrs'
                WHEN min_to_ob >= 1440 THEN 'mayor_24_hrs'
                ELSE 'otro'
            END AS tiempo_ob,
            CASE 
                WHEN min_to_fci IS NULL THEN 'sin_fci'
                WHEN min_to_fci	< 3 THEN 'menor_3_min'
                WHEN min_to_fci	>=3 AND min_to_fci < 5 THEN 'entre_3_5_min'
                WHEN min_to_fci	>= 5 AND min_to_fci	< 15 THEN 'entre_5_15_min'
                WHEN min_to_fci	>= 15 AND min_to_fci < 30 THEN 'entre_15_30_min'
                WHEN min_to_fci	>= 30 AND min_to_fci < 60 THEN 'entre_30_1_hr'
                WHEN min_to_fci	>= 60 AND min_to_fci < 180 THEN 'entre_1_3_hr'
                WHEN min_to_fci	>= 180 AND min_to_fci < 720 THEN 'entre_3_12_hrs'
                WHEN min_to_fci	>= 720 AND min_to_fci < 1440 THEN 'entre_12_24_hrs'
                WHEN min_to_fci >= 1440 THEN 'mayor_24_hrs'
                ELSE 'otro'
            END AS tiempo_to_fci
    FROM {{ ref('time_to_next_step_funnel')}} 
    WHERE fecha_ob IS NOT NULL -- SOLO OB's
    )

SELECT
    *
FROM 
(SELECT user, mes, tiempo_ob, tiempo_to_fci FROM data)
PIVOT(COUNT(distinct user)
FOR tiempo_to_fci IN
    ('sin_fci','menor_3_min','entre_3_5_min','entre_5_15_min','entre_15_30_min','entre_30_1_hr','entre_1_3_hr','entre_3_12_hrs','entre_12_24_hrs','mayor_24_hrs'))
ORDER  BY mes DESC, tiempo_ob DESC
