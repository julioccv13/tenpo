DROP TABLE IF EXISTS `tenpo-bi.tmp.ahorro_supuesto_regla_inversiones_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.ahorro_supuesto_regla_inversiones_{{ds_nodash}}` AS 
(
with pre_monto as
(
  select
    user,
    monto as monto_tx,
    case
      when monto < 1000 then 1000
      when (monto - round(monto,-3)) = 0 then monto
      when (monto - round(monto,-3)) < 0 then round(monto,-3) else (round(monto,-3)+1000) end as redondeo
  from `tenpo-bi-prod.economics.economics`
  where (extract(year from fecha)*100+extract(month from fecha)) = (extract(year from date_add(current_date(), interval -1 month))*100+extract(month from date_add(current_date(), interval -1 month)))
   and linea in ('mastercard_physical','mastercard','utility_payments')
    and lower(nombre) not like '%devoluc%'
    AND linea <> 'reward'
)
,tabla_final_monto as 
(
    select user,monto_tx, redondeo, (redondeo-monto_tx) as inversion_vuelto
    from pre_monto
)
,tabla_final_monto_2 as 
(
    select user as identity, ifnull(sum(inversion_vuelto),0) as ahorro_supuesto_regla_inversiones_compra
    from tabla_final_monto
    group by 1
)
,pre_cashin as
(
  select
    user,
    monto as monto_tx,
    monto * 0.05 as inversion,
  from `tenpo-bi-prod.economics.economics`
  where (extract(year from fecha)*100+extract(month from fecha)) = (extract(year from date_add(current_date(), interval -1 month))*100+extract(month from date_add(current_date(), interval -1 month)))
   and linea in ('cash_in')
)
,tabla_final_cashin as 
(
    select distinct user as identity, cast(IFNULL(sum(inversion),0) as int) as ahorro_supuesto_regla_inversiones_cashin
    from pre_cashin
    group by 1
),final as (
select distinct
      T1.id as identity,
      T2.ahorro_supuesto_regla_inversiones_compra as ahorro_supuesto_regla_inversiones_compra,
      T3.ahorro_supuesto_regla_inversiones_cashin as ahorro_supuesto_regla_inversiones_cashin,
      T2.ahorro_supuesto_regla_inversiones_compra + T3.ahorro_supuesto_regla_inversiones_cashin as ahorro_supuesto_sumado_regla_inversiones_cashin_y_compras
from `tenpo-bi-prod.users.users_tenpo` T1
  left join tabla_final_monto_2 T2 on T1.id = T2.identity
  left join tabla_final_cashin T3 on T1.id = T3.identity
where T1.status_onboarding = 'completo'
), final_pivot as (
select distinct identity, 
CONCAT('$', REPLACE(FORMAT('%\'d', CAST(ahorro_supuesto_regla_inversiones_compra AS INT64)), ',', '.')) as ahorro_supuesto_regla_inversiones_compra,
CONCAT('$', REPLACE(FORMAT('%\'d', CAST(ahorro_supuesto_regla_inversiones_cashin AS INT64)), ',', '.')) as ahorro_supuesto_regla_inversiones_cashin,
CONCAT('$', REPLACE(FORMAT('%\'d', CAST(ahorro_supuesto_sumado_regla_inversiones_cashin_y_compras AS INT64)), ',', '.')) as ahorro_supuesto_sumado_regla_inversiones_cashin_y_compras
from final
)
select distinct identity,
case when ahorro_supuesto_regla_inversiones_compra is null then '0' else ahorro_supuesto_regla_inversiones_compra end as ahorro_supuesto_regla_inversiones_compra,

case when ahorro_supuesto_regla_inversiones_cashin is null then '0' else ahorro_supuesto_regla_inversiones_cashin end as ahorro_supuesto_regla_inversiones_cashin,

case when ahorro_supuesto_sumado_regla_inversiones_cashin_y_compras is null then '0' else ahorro_supuesto_sumado_regla_inversiones_cashin_y_compras end as ahorro_supuesto_sumado_regla_inversiones_cashin_y_compras

from final_pivot
)