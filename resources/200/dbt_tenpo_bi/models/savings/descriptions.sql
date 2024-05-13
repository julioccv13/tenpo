{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}

WITH savings_balance AS 

(
SELECT
    DISTINCT
    user_id user,
    ROUND(current_fee_quantity * (SELECT amount FROM {{ source('payment_savings', 'fees') }} ORDER BY valid_on_date DESC LIMIT 1)) as saldo_bolsillo
FROM {{ source('payment_savings', 'user_position') }} savings
--JOIN `tenpo-airflow-prod.users.users` users on users.id = savings.user_id
JOIN {{ source('tenpo_users', 'users') }} users on users.id = savings.user_id
WHERE TRUE
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY calculate_date DESC) = 1

),


descriptivos AS (

    SELECT 
        user,
        LAST_VALUE(monto) OVER (PARTITION BY user ORDER BY dt_trx_chile DESC) as last_investment,
        MAX(monto) OVER (PARTITION BY user) as highest_investment,
        ROUND(AVG(monto) OVER (PARTITION BY user)) as avg_investment,
        COUNT(trx_id) OVER (PARTITION BY user) as cantidad_de_aportes,
        SUM(monto) OVER (PARTITION BY user) as monto_total_invertido,
    FROM {{ ref('economics') }}


    WHERE linea = 'cash_in_savings'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY dt_trx_chile DESC) = 1
    ORDER BY dt_trx_chile DESC

), 

rescates AS (

    SELECT 
        DISTINCT
        user,
        COUNT(trx_id) OVER (PARTITION BY user) as cantidad_de_rescates,
    FROM {{ ref('economics') }}
    WHERE linea = 'cash_out_savings'
   

)

SELECT 
    DISTINCT
    savings_balance.user
    ,savings_balance.saldo_bolsillo 
    ,descriptivos.last_investment
    ,descriptivos.highest_investment
    ,descriptivos.avg_investment
    ,descriptivos.cantidad_de_aportes
    ,IFNULL(rescates.cantidad_de_rescates,0) cantidad_de_rescates
    ,descriptivos.monto_total_invertido
    ,CASE -- mantener de 10 en 10, categorial plot: https://matplotlib.org/stable/tutorials/introductory/pyplot.html
        WHEN saldo_bolsillo = 0 then 'Saldo 0'
        WHEN saldo_bolsillo > 0 and saldo_bolsillo <= 10000 then '0 < Saldo <= 10.000'
        WHEN saldo_bolsillo > 10000 and saldo_bolsillo <= 20000 then '10.000 < Saldo <= 20.000'
        WHEN saldo_bolsillo > 20000 and saldo_bolsillo <= 30000 then '20.000 < Saldo <= 30.000'
        WHEN saldo_bolsillo > 30000 and saldo_bolsillo <= 40000 then '30.000 < Saldo <= 40.000'
        WHEN saldo_bolsillo > 40000 and saldo_bolsillo <= 500000 then '40.000 < Saldo <= 50.000'
        WHEN saldo_bolsillo > 500000 and saldo_bolsillo <= 60000 then '50.000 < Saldo <= 60.000'
        WHEN saldo_bolsillo > 600000 and saldo_bolsillo <= 70000 then '60.000 < Saldo <= 70.000'
        WHEN saldo_bolsillo > 700000 and saldo_bolsillo <= 80000 then '70.000 < Saldo <= 80.000'
        WHEN saldo_bolsillo > 800000 and saldo_bolsillo <= 90000 then '80.000 < Saldo <= 90.000'
        WHEN saldo_bolsillo > 900000 and saldo_bolsillo <= 100000 then '90.000 < Saldo <= 100.000'
        ELSE ' Mayor a 100mil'   
    END AS categoria
FROM savings_balance
JOIN descriptivos USING (user)
LEFT JOIN rescates USING (user)