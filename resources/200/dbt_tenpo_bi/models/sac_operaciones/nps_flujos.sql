{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}
SELECT 
    date(DATE_TRUNC(A.submitted_at,MONTH)) mes,
    A.submitted_at,
    A.source,
    B.id as user,
    A.score,
    if (A.score >= 9,1,0) as promotor,
    if (A.score <= 6,1,0) as detractor,
    A.feedback,
    A.sentiment_score
FROM {{source('nps','flow_evaluations')}} A
JOIN {{source('tenpo_users','users')}} B USING (rut)
ORDER BY 2 desc