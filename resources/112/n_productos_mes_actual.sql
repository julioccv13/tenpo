DROP TABLE IF EXISTS `tenpo-bi.tmp.n_productos_cliente_mes_actual_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.n_productos_cliente_mes_actual_{{ds_nodash}}` AS (
with
economics as 
(
  select user, 
    case 
      when linea in ('credit_card_virtual','credit_card_physical') then 'credit_card' else linea end as linea
  FROM `tenpo-bi-prod.economics.economics` a
  WHERE
      (EXTRACT(year from fecha)*100 + EXTRACT(month from fecha)) = (EXTRACT(year from CURRENT_DATE())*100 + EXTRACT(month from CURRENT_DATE()))
      --and linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','investment_tyba','insurance','utility_payments')
      and linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','insurance','utility_payments','top_ups','credit_card_virtual','credit_card_physical')
      --and linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','insurance','top_ups','credit_card_virtual','credit_card_physical')
      AND linea NOT LIKE '%PFM%'
      AND nombre NOT LIKE '%Home%'
      AND linea != 'Saldo'
      AND linea != 'saldo'
      and lower(nombre) not like '%devoluc%'
      AND linea <> 'reward'
  /*
  union all 

  select user_id as user, 'regla_automatica_inversion' as linea
  from `tenpo-airflow-prod.savings_rules.rules` 
  where (EXTRACT(year from created_at)*100 + EXTRACT(month from created_at)) = (EXTRACT(year from CURRENT_DATE())*100 + EXTRACT(month from CURRENT_DATE()))
  */
)

,
users_producto as (
  SELECT
    a.user as identity,
    count(distinct a.linea) as n_productos_mes_actual
  FROM economics a
  GROUP BY 1
)

select
      a.id as identity
      ,ifnull(b.n_productos_mes_actual,0) as n_productos_mes_actual
      ,(11 - ifnull(b.n_productos_mes_actual,0)) as n_productos_mes_actual_faltante_hasta_10
from `tenpo-bi-prod.users.users_tenpo` a
  left join users_producto b on a.id = b.identity 
where status_onboarding = 'completo'

)


