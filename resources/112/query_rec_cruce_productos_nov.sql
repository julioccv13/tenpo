DROP TABLE IF EXISTS `tenpo-bi.tmp.rec_cruce_productos_nov_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.rec_cruce_productos_nov_{{ds_nodash}}` AS 
(
with
base as (
  select distinct user,
  n_productos as cruce_nov_n_productos,
  coalesce(listado_productos,'de momento no haz usado productos Tenpo') as cruce_nov_listado_productos
  from `tenpo-sandbox.crm.avance_campana_cruce_noviembre`
),
rec_0 as (
  select user,
  case when prod_1_reactivacion in ('cash_in_savings','investment_tyba','paypal','p2p_received') then null else prod_1_reactivacion end as prod_1_reactivacion,
  case when prod_2_reactivacion in ('cash_in_savings','investment_tyba','paypal','p2p_received') then null else prod_2_reactivacion end as prod_2_reactivacion,
  case when prod_3_reactivacion in ('cash_in_savings','investment_tyba','paypal','p2p_received') then null else prod_3_reactivacion end as prod_3_reactivacion,
  case when recomendacion_1 in ('cash_in_savings','investment_tyba','paypal','p2p_received') then null else recomendacion_1 end as recomendacion_1,
  case when recomendacion_2 in ('cash_in_savings','investment_tyba','paypal','p2p_received') then null else recomendacion_2 end as recomendacion_2,
  case when recomendacion_3 in ('cash_in_savings','investment_tyba','paypal','p2p_received') then null else recomendacion_3 end as recomendacion_3,
  from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`
  where fecha_ejecucion=(select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`)
),
rec_1 as (
  select user,
  coalesce(prod_1_reactivacion, prod_2_reactivacion, prod_3_reactivacion,recomendacion_1,recomendacion_2,recomendacion_3) as rec1
  from rec_0
),
rec_2 as (
  select a.user, b.rec1,
  coalesce(
    case when prod_1_reactivacion=b.rec1 then null else prod_1_reactivacion end,
    case when prod_2_reactivacion=b.rec1 then null else prod_2_reactivacion end,
    case when prod_3_reactivacion=b.rec1 then null else prod_3_reactivacion end,
    case when recomendacion_1=b.rec1 then null else recomendacion_1 end,
    case when recomendacion_2=b.rec1 then null else recomendacion_2 end,
    case when recomendacion_3=b.rec1 then null else recomendacion_3 end
  ) as rec2
  from rec_0 a
  left join rec_1 b on a.user=b.user
),
rec_3 as (
  select a.user, b.rec1, c.rec2,
  coalesce(
    case when prod_1_reactivacion=b.rec1 or prod_1_reactivacion=c.rec2 then null else prod_1_reactivacion end,
    case when prod_2_reactivacion=b.rec1 or prod_2_reactivacion=c.rec2 then null else prod_2_reactivacion end,
    case when prod_3_reactivacion=b.rec1 or prod_3_reactivacion=c.rec2 then null else prod_3_reactivacion end,
    case when recomendacion_1=b.rec1 or recomendacion_1=c.rec2 then null else recomendacion_1 end,
    case when recomendacion_2=b.rec1 or recomendacion_2=c.rec2 then null else recomendacion_2 end,
    case when recomendacion_3=b.rec1 or recomendacion_3=c.rec2 then null else recomendacion_3 end
  ) as rec3
  from rec_0 a
  left join rec_1 b on a.user=b.user
  left join rec_2 c on a.user=c.user
),
recomendaciones as (
  select *
  from rec_3
  where rec1 is not null and rec2 is not null and rec3 is not null
),
base_final_1 as (
  select a.user, cruce_nov_n_productos, cruce_nov_listado_productos,
  case
    when rec1='mastercard' then 'Tarjeta Prepago Digital'
    when rec1='utility_payments' then 'Pago de Cuentas'
    when rec1='mastercard_physical' then 'Tarjeta Prepago Física'
    --when rec1='cash_in_savings' then 'Activación y Uso Regla Automática Bolsillo'
    when rec1='crossborder' then 'Transferencias al extranjero'
    when rec1='paypal_abonos' then 'Billetera en dólares'
    when rec1='p2p' then 'Transferencias entre amigos'
    when rec1='top_ups' then 'Recargas'
    else null end as rec1,
  case
    when rec2='mastercard' then 'Tarjeta Prepago Digital'
    when rec2='utility_payments' then 'Pago de Cuentas'
    when rec2='mastercard_physical' then 'Tarjeta Prepago Física'
    --when rec2='cash_in_savings' then 'Activación y Uso Regla Automática Bolsillo'
    when rec2='crossborder' then 'Transferencias al extranjero'
    when rec2='paypal_abonos' then 'Billetera en dólares'
    when rec2='p2p' then 'Transferencias entre amigos'
    when rec2='top_ups' then 'Recargas'
    else null end as rec2,
  case
    when rec3='mastercard' then 'Tarjeta Prepago Digital'
    when rec3='utility_payments' then 'Pago de Cuentas'
    when rec3='mastercard_physical' then 'Tarjeta Prepago Física'
    --when rec3='cash_in_savings' then 'Activación y Uso Regla Automática Bolsillo'
    when rec3='crossborder' then 'Transferencias al extranjero'
    when rec3='paypal_abonos' then 'Billetera en dólares'
    when rec3='p2p' then 'Transferencias entre amigos'
    when rec3='top_ups' then 'Recargas'
    else null end as rec3,
  from base a
  left join recomendaciones b on a.user=b.user
),
base_final_2 as (
select user, cruce_nov_n_productos, cruce_nov_listado_productos,
coalesce(rec1,'Transferencias al extranjero') as rec1,
coalesce(rec2,'Billetera en dólares') as rec2,
coalesce(rec3,'Seguros') as rec3,
from base_final_1 a
)
select user as identity,
cruce_nov_n_productos,
cruce_nov_listado_productos,
rec1||', '||rec2||', '||rec3 as cruce_nov_recomendacion
from base_final_2
)