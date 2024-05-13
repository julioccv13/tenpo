DROP TABLE IF EXISTS `tenpo-bi.tmp.n_productos_cliente_mes_actual_recomendacion_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.n_productos_cliente_mes_actual_recomendacion_{{ds_nodash}}` AS (
with
traduccion_producto as 
(
  select 'mastercard' as linea, 'tarjeta digital' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/td.png' as link
  union all 
  select 'mastercard_physical' as linea, 'tarjeta fisica' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/tf.png' as link
  union all 
  select 'p2p' as linea, 'transferencia a tenpista' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/tr.png' as link
  union all 
  select 'crossborder' as linea, 'remesas' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/tae.png' as link
  union all 
  select 'paypal_abonos' as linea, 'abono dolares' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/adp.png' as link
  union all 
  select 'paypal' as linea, 'retiro dolares' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/rdp.png' as link
  union all 
  select 'cash_in_savings' as linea, 'inversion bolsillo' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/ara.png' as link
  --union all 
  --select 'investment_tyba' as linea, 'inversion ffmm' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/ara.png' as link
  union all 
  select 'insurance' as linea, 'seguros' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/seg.png' as link
  union all 
  select 'utility_payments' as linea, 'pago de cuenta' as producto, 'https://tenpoimagesstorage2.blob.core.windows.net/marketing/04_ct/2/pdc.png' as link  
)
,economics as 
(
  select distinct a.user, b.producto 
  FROM `tenpo-bi-prod.economics.economics` a
    left join traduccion_producto b on a.linea = b.linea
  WHERE
      (EXTRACT(year from fecha)*100 + EXTRACT(month from fecha)) = (EXTRACT(year from CURRENT_DATE())*100 + EXTRACT(month from CURRENT_DATE()))
      and a.linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','investment_tyba','insurance','utility_payments')
      --and a.linea in ('mastercard','mastercard_physical','p2p','crossborder','paypal_abonos','paypal','cash_in_savings','insurance','utility_payments')
      AND a.linea NOT LIKE '%PFM%'
      AND a.nombre NOT LIKE '%Home%'
      AND a.linea != 'Saldo'
      AND a.linea != 'saldo'
      and lower(a.nombre) not like '%devoluc%'
      AND a.linea <> 'reward'

)
,listado_total_productos as 
(
    select a.id as user, b.producto
    from `tenpo-bi-prod.users.users_tenpo` a
        left join (
            SELECT * 
            FROM UNNEST(
                ["tarjeta digital","pago de cuenta","seguros","inversion bolsillo","retiro dolares","abono dolares","remesas","transferencia a tenpista","tarjeta fisica"]) AS producto
        ) b on 1 = 1
    where a.status_onboarding = 'completo'
        --and a.id = '17097bbc-2c37-412a-a9e6-87aef9c10026'
)
,listado_productos_pendientes_mes as 
(
    select a.user, a.producto
    from listado_total_productos a  
        left join economics b on (a.user = b.user and a.producto = b.producto)
    where b.producto is null
)
,score_productos_pre_data as 
(
    select distinct user, prod_1_reactivacion as producto, (score_reactivacion_1+100) as score, 'reactivar' as tipo
    from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` 
    where fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`) and  prod_1_reactivacion is not null and prod_1_reactivacion not in ('investment_tyba')
    union all   
    select distinct user, prod_2_reactivacion as producto, (score_reactivacion_2+100) as score, 'reactivar' as tipo
    from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` 
    where fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`) and  prod_2_reactivacion is not null and prod_2_reactivacion not in ('investment_tyba')
    union all 
    select distinct user, prod_3_reactivacion as producto, (score_reactivacion_3+100) as score, 'reactivar' as tipo
    from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` 
    where fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`) and  prod_3_reactivacion is not null and prod_3_reactivacion not in ('investment_tyba')
    union all 
    select distinct user, recomendacion_1 as producto, (score_recomendacion_1) as score, 'recomendar' as tipo
    from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` 
    where fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`) and  recomendacion_1 is not null and recomendacion_1 not in ('investment_tyba')
    union all 
    select distinct user, recomendacion_2 as producto, (score_recomendacion_2) as score, 'recomendar' as tipo
    from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` 
    where fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`) and  recomendacion_2 is not null and recomendacion_2 not in ('investment_tyba')
    union all 
    select distinct user, recomendacion_3 as producto, (score_recomendacion_3) as score, 'recomendar' as tipo
    from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` 
    where fecha_ejecucion = (select max(fecha_ejecucion) from `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`) and  recomendacion_3 is not null and recomendacion_3 not in ('investment_tyba')   
)
,score_productos_pre as 
(
  select user, producto, max(score) as score, max(tipo) as tipo
  from score_productos_pre_data
  group by 1,2
)
,score_productos as 
(
  select a.user, b.producto, a.score, a.tipo
  from score_productos_pre a 
    left join traduccion_producto b on a.producto = b.linea
)
,score_producto_default as 
(
  select 'mastercard' as linea, 'tarjeta digital' as producto, 0.001 as score, 'default' as tipo
  union all  
  select 'mastercard_physical' as linea, 'tarjeta fisica' as producto, 0.002 as score, 'default' as tipo
  union all 
  select 'p2p' as linea, 'transferencia a tenpista' as producto, 0.003 as score, 'default' as tipo
  union all 
  select 'crossborder' as linea, 'remesas' as producto, 0.007 as score, 'default' as tipo
  union all 
  select 'paypal_abonos' as linea, 'abono dolares' as producto, 0.009 as score, 'default' as tipo
  union all 
  select 'paypal' as linea, 'retiro dolares' as producto, 0.010 as score, 'default' as tipo
  union all 
  select 'cash_in_savings' as linea, 'inversion bolsillo' as producto, 0.004 as score, 'default' as tipo
  --union all 
  --select 'investment_tyba' as linea, 'inversion ffmm' as producto, 0.005 as score, 'default' as tipo
  union all 
  select 'insurance' as linea, 'seguros' as producto, 0.008 as score, 'default' as tipo
  union all 
  select 'utility_payments' as linea, 'pago de cuenta' as producto  , 0.006 as score, 'default' as tipo
)
,listado_productos_pendientes_mes_score as 
(
  select 
    a.user, 
    a.producto,
    ifnull(b.score,c.score) as score ,
    ifnull(b.tipo,c.tipo) as tipo 
  from listado_productos_pendientes_mes a
    left join score_productos b on (a.user = b.user and a.producto = b.producto)
    left join score_producto_default c on (a.producto = c.producto)
)
,listado_productos_pendientes_mes_score_ordenado as 
(
  select 
    user,
    producto,
    score,
    tipo,
    ROW_NUMBER() OVER(PARTITION BY user ORDER BY score desc) as posicion  
  from listado_productos_pendientes_mes_score
  --where user = '17097bbc-2c37-412a-a9e6-87aef9c10026'
)
,pre_final as 
(
select distinct
  a.user as identity,
  b.producto as n_productos_mes_actual_recomendacion1,
  c.producto as n_productos_mes_actual_recomendacion2,
  d.producto as n_productos_mes_actual_recomendacion3,
  e.producto as n_productos_mes_actual_recomendacion4,
  f.producto as n_productos_mes_actual_recomendacion5,
from listado_productos_pendientes_mes_score_ordenado  a 
  left join (select distinct user, producto from listado_productos_pendientes_mes_score_ordenado where posicion = 1) b on a.user = b.user
  left join (select distinct user, producto from listado_productos_pendientes_mes_score_ordenado where posicion = 2) c on a.user = c.user
  left join (select distinct user, producto from listado_productos_pendientes_mes_score_ordenado where posicion = 3) d on a.user = d.user
  left join (select distinct user, producto from listado_productos_pendientes_mes_score_ordenado where posicion = 4) e on a.user = e.user
  left join (select distinct user, producto from listado_productos_pendientes_mes_score_ordenado where posicion = 5) f on a.user = f.user
)
select 
  a.identity,
  b.link as n_productos_mes_actual_recomendacion1,
  c.link as n_productos_mes_actual_recomendacion2,
  d.link as n_productos_mes_actual_recomendacion3,
  e.link as n_productos_mes_actual_recomendacion4,
  f.link as n_productos_mes_actual_recomendacion5,
  --a.n_productos_mes_actual_recomendacion1 as prod_1,
  --a.n_productos_mes_actual_recomendacion2 as prod_2,
  --a.n_productos_mes_actual_recomendacion3 as prod_3,
  --a.n_productos_mes_actual_recomendacion4 as prod_4,
  --a.n_productos_mes_actual_recomendacion5 as prod_5,
from pre_final a 
  left join (select distinct producto, link from traduccion_producto) b on a.n_productos_mes_actual_recomendacion1 = b.producto
  left join (select distinct producto, link from traduccion_producto) c on a.n_productos_mes_actual_recomendacion2 = c.producto
  left join (select distinct producto, link from traduccion_producto) d on a.n_productos_mes_actual_recomendacion3 = d.producto
  left join (select distinct producto, link from traduccion_producto) e on a.n_productos_mes_actual_recomendacion4 = e.producto
  left join (select distinct producto, link from traduccion_producto) f on a.n_productos_mes_actual_recomendacion5 = f.producto
)
