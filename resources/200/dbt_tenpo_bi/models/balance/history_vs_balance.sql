
{{ config(tags=["weekly", "bi"], materialized='table') }}

WITH data_history AS
  
  ( WITH data AS     
        
        (SELECT 
              user_id,
              transaction_id, 
              case 
                  when type.id = 2 and total_currency_value > 0 then total_currency_value * (-1)
                  else total_currency_value 
              end as total_currency_value,
              created_at
        FROM {{ source('transactions_history', 'transactions_history') }} hist
        JOIN {{ source('transactions_history', 'types') }} type on hist.type_id = type.id
        and visible is true
        )

        SELECT 
              user_id, 
              SUM(total_currency_value) saldo_history,
              COUNT(*) transacciones,
              max(date(created_at)) fecha_ultima_trx
        FROM data
        GROUP BY 1
  
  ),
  balance AS (

    SELECT 
          user,
          saldo_dia as ultimo_saldo_reportado
    FROM {{ ref('daily_balance') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY fecha DESC) = 1 

  ), qualify_data AS 

    (SELECT
        data_history.*,
        balance.ultimo_saldo_reportado,
        case when saldo_history < 0 and ultimo_saldo_reportado = 0 then true else false end as caso_saldo_negativo
    FROM data_history
    JOIN balance ON balance.user = data_history.user_id
    where fecha_ultima_trx < DATE_SUB(DATE(current_date()), INTERVAL 2 DAY)
    )

select 
      date_trunc(date(ob_completed_at_dt_chile),month) mes_ob,
      date(users.ob_completed_at_dt_chile) fecha_ob,
      qualify_data.* 
from qualify_data
join {{ ref('users_tenpo') }} users ON users.id = qualify_data.user_id
where saldo_history <> ultimo_saldo_reportado
