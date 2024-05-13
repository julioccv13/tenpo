SELECT distinct
'i' as type, identity as identity
FROM
  `{project_source_2}.clevertap.events` 
WHERE
  event = 'App Launched'
  AND 
  EXTRACT(date from fecha_hora) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
  and ct_app_v != ( '3.3.8')
