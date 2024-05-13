{{ config(materialized='table',  tags=["daily", "bi"]) }}

with 
/*
select distinct
id_cuenta,
timestamp_diff(lead(fec_fechahora_envio) OVER (PARTITION BY id_cuenta ORDER BY fec_fechahora_envio), fec_fechahora_envio,  DAY) as dias_entre_trx,
*/
  topups as(    
  SELECT
    t.user_id as user,
    r.identifier as identificador,
    pr.name as producto,
    o.name as operador,
    timestamp_diff(lead(t.created_at) OVER (PARTITION BY t.user_id,pr.name,o.name,r.identifier ORDER BY t.created_at), t.created_at,  DAY) as dias_entre_trx,
    count(distinct r.id) OVER (PARTITION BY t.user_id,pr.name,o.name,r.identifier) as cant_trx,
    avg(amount) OVER (PARTITION BY t.user_id,pr.name,o.name,r.identifier) as prom_monto,
    'top_ups' as linea,
  FROM {{source('tenpo_topup','topup')}}  r
    JOIN {{source('tenpo_topup','product_operator')}} p ON r.product_operator_id = p.id
    JOIN {{source('tenpo_topup','operator')}}  o ON o.id= p.operator_id
    JOIN {{source('tenpo_topup','transaction')}} t ON r.transaction_id = t.id
    JOIN {{source('tenpo_topup','product')}} pr ON p.product_id = pr.id
  WHERE r.status  = 'SUCCEEDED'
    AND t.status ='SUCCEEDED'
  ORDER BY 1
  ),
  
  utilities as(    
  SELECT
    b.user as user,
    b.identifier as identificador,
    c.name as producto,
    u.name as operador,
    timestamp_diff(lead(b.created ) OVER (PARTITION BY b.user, c.name ,u.name,b.identifier  ORDER BY b.created), b.created,  DAY) as dias_entre_trx,
    count(distinct b.id ) OVER (PARTITION BY b.user, c.name ,u.name,b.identifier) as cant_trx,
    avg(amount) OVER (PARTITION BY b.user, c.name ,u.name,b.identifier) as prom_monto,
    'utility_payments' as linea,
  FROM {{source('tenpo_utility_payment','bills')}} b
        INNER JOIN  {{source('tenpo_utility_payment','utilities')}} u ON b.utility_id = u.id
        INNER JOIN  {{source('tenpo_utility_payment','categories')}} c ON c.id = u.category_id 
  WHERE b.status = 'SUCCEEDED'
  ORDER BY 1
  )
  select distinct
    linea,
    user,
    producto, 
    operador,
    identificador,
    cant_trx,
    prom_monto,
    ROUND(avg(dias_entre_trx) OVER (PARTITION BY linea,user,producto,operador,identificador)) as prom_dias_entre_trx,
  from (
  SELECT * FROM topups
  UNION ALL 
  SELECT * FROM utilities
  )
  as datos
  order by 1