WITH segmento AS (
select distinct
user
from `{project_source_1}.economics.economics`
where
linea = 'aum_savings'
and monto >= 50000
and fecha = (select max(fecha) from `{project_source_1}.economics.economics` where linea = 'aum_savings')
and user not in  (
select distinct
  user
from `{project_source_1}.economics.economics`
where
linea = 'cash_in_savings'
and monto >= 1
and fecha >= date_add(CURRENT_DATE(),INTERVAL -1 MONTH) )
),

USUARIOS_UNICOS AS (
    SELECT 
    DISTINCT(user)
    FROM segmento 
)

SELECT
    'i' AS type,
    user AS identity
FROM USUARIOS_UNICOS