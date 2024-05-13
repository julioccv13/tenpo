{{ 
    config(
        materialized='table'
        ,tags=["daily", "bi"]
        ,project=env_var('DBT_PROJECT24', 'tenpo-datalake-sandbox')
        ,schema='insumos_jarvis'
        ,alias='campana_rf_recargafacil'
    )
}}

WITH  EMAIL_TELEFONO AS
(
SELECT
CORREO_USUARIO EMAIL,
COUNT(DISTINCT ID_SUS) CANT_NUM,
STRING_AGG(DISTINCT ID_SUS) NUMEROS
FROM {{ ref('funnel_recargas_sin_ofuscar') }}
WHERE 
TRX_ESTADO = '5. Finalizado'
AND CATEGORIA = 'Telefonía Móvil'
AND replace(CORREO_USUARIO, ' ', '') <> '' 
GROUP BY 1 
HAVING CANT_NUM <=10
),

RECARGAFACIL AS
(
SELECT 
CORREO_USUARIO EMAIL,
DATE_DIFF(CURRENT_DATE(),MAX(SAFE_CAST(FECHA AS DATE)), DAY) AS R,
COUNT(*) AS F,
SUM(MONTO) AS M 
FROM {{ ref('funnel_recargas_sin_ofuscar') }}
WHERE 
TRX_ESTADO = '5. Finalizado'
GROUP BY CORREO_USUARIO
),

USERSTEMPO AS
(
SELECT 
EMAIL,
PHONE
FROM  {{ ref('users_tenpo_sin_ofuscar') }}
WHERE
STATE IN (4,7,8,21,22)
)

SELECT 
A.*, 
B.NUMEROS,
CASE 
WHEN R BETWEEN 0 AND 30 THEN '0 A 30'
WHEN R BETWEEN 31 AND 60 THEN '31 A 60'
WHEN R BETWEEN 61 AND 90 THEN '61 A 90'
ELSE '91+'
END INTERVALOS
FROM RECARGAFACIL  A 
LEFT JOIN EMAIL_TELEFONO  B USING(EMAIL)
WHERE 
EMAIL NOT IN (SELECT EMAIL FROM USERSTEMPO) 