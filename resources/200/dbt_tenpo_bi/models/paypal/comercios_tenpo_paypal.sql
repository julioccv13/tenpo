{{ config(materialized='table',  tags=["hourly", "bi"]) }}

SELECT
  replace(replace(IF(UPPER(comercio) like "%EBAY%","EBAY", UPPER(comercio)), "PAYPAL *", ""),"PP* ","") as merchant, 
  user AS users,
  sum(monto) AS ammount,
  count(1) AS count_trx,
  DATE(EXTRACT(YEAR FROM fecha),EXTRACT(MONTH FROM fecha),1) AS month
FROM {{ref('economics')}}
WHERE TRUE
  AND upper(comercio_recod) like "PAYPAL"
  AND comercio NOT LIKE "PP%CODE"
GROUP BY merchant,month,user
ORDER BY 1