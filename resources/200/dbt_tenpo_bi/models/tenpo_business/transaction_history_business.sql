
{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

with table_users_business as(SELECT uuid,CAST(cuenta AS STRING) as cuenta,estado,tipo
FROM {{source('prepago','prp_cuenta')}}
WHERE (tipo ='FREELANCE' OR tipo = 'BUSINESS') and cuenta is not null anD estado = 'ACTIVE')


select t2.transaction_id,t.uuid ,t2.user_id,t.estado,t.tipo,t2.total_currency_value,t2.card_id,t2.created_at as date_transaction, t2.state_id,t2.category,case when t2.total_currency_value>0 then 'Cash_in' else 'Cash_out' end as cash
FROM table_users_business AS t
left join {{source('transactions_history','transactions_history')}} AS t2
ON t.uuid = t2.account_id