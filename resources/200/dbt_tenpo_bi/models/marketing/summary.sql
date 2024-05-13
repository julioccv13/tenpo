{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}

WITH onboardings AS 

(SELECT  
    fecha,
    COUNT(DISTINCT user) onboardings
FROM {{ref('onboardings')}}
GROUP BY 1
),

first_cashins AS (

SELECT  
    fecha,
    COUNT(DISTINCT user) first_cashins
FROM {{ref('first_cashin')}}
GROUP BY 1

),

first_transactions AS (

SELECT  
    fecha,
    COUNT(DISTINCT user) first_transactions
FROM {{ref('first_transaction')}}
GROUP BY 1

)

SELECT 
    onboardings.fecha,
    onboardings.onboardings,
    first_cashins.first_cashins,
    first_transactions.first_transactions

FROM onboardings
JOIN first_cashins USING (fecha)
JOIN first_transactions USING (fecha)
ORDER BY 1 DESC