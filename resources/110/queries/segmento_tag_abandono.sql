WITH segmento AS (
SELECT distinct
  eco.user
FROM
  `{project_source_1}.economics.economics` AS eco
WHERE
  eco.fecha >= DATE_ADD(CURRENT_DATE(),INTERVAL -24 MONTH)
  AND eco.nombre = 'Autopistas'
  AND eco.user NOT IN (
  SELECT
    DISTINCT (eco.user)
  FROM
    `{project_source_1}.economics.economics` AS eco
  WHERE
  eco.nombre = 'Autopistas'
  AND eco.fecha >= DATE_ADD(CURRENT_DATE(),INTERVAL -2 MONTH)
    )
)

SELECT
    'i' AS type,
    user AS identity
FROM segmento