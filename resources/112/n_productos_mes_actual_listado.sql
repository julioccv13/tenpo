DROP TABLE IF EXISTS `tenpo-bi.tmp.n_productos_cliente_mes_actual_listado_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.n_productos_cliente_mes_actual_listado_{{ds_nodash}}` AS (
with
traduccion_producto as 
(
  select 'mastercard' as linea, 'tarjeta digital' as producto, 'tarjeta digital' as nombre_oficial
  union all 
  select 'mastercard_physical' as linea, 'tarjeta fisica' as producto, 'tarjeta fisica' as nombre_oficial
  union all 
  select 'p2p' as linea, 'transferencia a tenpista' as producto, 'transferencia a tenpista' as nombre_oficial
  union all 
  select 'crossborder' as linea, 'remesas' as producto, 'remesas' as nombre_oficial
  union all 
  select 'paypal_abonos' as linea, 'abono dolares' as producto, 'abono dolares' as nombre_oficial
  union all 
  select 'paypal' as linea, 'retiro dolares' as producto, 'retiro dolares' as nombre_oficial
  union all 
  select 'cash_in_savings' as linea, 'inversion bolsillo' as producto, 'activación y uso de regla automática en bolsillo' as nombre_oficial
  --union all 
  --select 'investment_tyba' as linea, 'inversion ffmm' as producto, 'inversion ffmm' as nombre_oficial
  union all 
  select 'insurance' as linea, 'seguros' as producto, 'seguros' as nombre_oficial
  union all 
  select 'utility_payments' as linea, 'pago de cuenta' as producto, 'pago de cuenta' as nombre_oficial  
  union all 
  select 'top_ups' as linea, 'recargas' as producto, 'recargas' as nombre_oficial  
  union all 
  select 'credit_card_virtual' as linea, 'tarjeta credito' as producto, 'tarjeta credito' as nombre_oficial  
  union all 
  select 'credit_card_physical' as linea, 'tarjeta credito' as producto, 'tarjeta credito' as nombre_oficial  
)
,economics as 
(
  select distinct a.user,b.nombre_oficial as producto--, b.producto 
  FROM `tenpo-bi-prod.economics.economics` a
    left join traduccion_producto b on a.linea = b.linea
  WHERE
      (EXTRACT(year from fecha)*100 + EXTRACT(month from fecha)) = (EXTRACT(year from CURRENT_DATE())*100 + EXTRACT(month from CURRENT_DATE()))
      --and a.linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','investment_tyba','insurance','utility_payments')
      and a.linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','insurance','utility_payments','top_ups','credit_card_virtual','credit_card_physical')
      --and a.linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','insurance','top_ups','credit_card_virtual','credit_card_physical')
      AND a.linea NOT LIKE '%PFM%'
      AND a.nombre NOT LIKE '%Home%'
      AND a.linea != 'Saldo'
      AND a.linea != 'saldo'
      and lower(a.nombre) not like '%devoluc%'
      AND a.linea <> 'reward'
  order by 2 asc

)
,
users_producto as (
  SELECT
    a.user as identity,
     STRING_AGG(a.producto, ", ") as n_productos_mes_actual_listado
    --count(distinct a.linea) as n_productos_mes_actual
  FROM economics a
  GROUP BY 1
)
select
      a.id as identity
      ,ifnull(b.n_productos_mes_actual_listado,'De momento no haz usado servicios Tenpo') as n_productos_mes_actual_listado
from `tenpo-bi-prod.users.users_tenpo` a
  left join users_producto b on a.id = b.identity 
where status_onboarding = 'completo'
)
