DROP TABLE IF EXISTS `${project_target}.tmp.monto_promedio_property_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.monto_promedio_property_{{ds_nodash}}` AS (
WITH
TRANSACCIONES AS 
(
  select
  user,
  fecha,
  trx_timestamp,
  linea,
  nombre,
  comercio,
  CONCAT(linea,'|',nombre, '|', comercio) as TRANSACCION,
  monto
  from `${project_source_1}.economics.economics` 
  where 
  fecha >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH)
  and user not in (select distinct id from `${project_source_2}.users.users` where EXTRACT(date from ob_completed_at) >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH))
  and linea not in ('p2p','p2p_received','reward','aum_savings','aum_tyba','withdrawal_tyba','cash_out_savings','cash_out')
  and nombre not in ('Devolución compra nacional','Devolución compra','Devolución compra peso')
)
,CONTEO_TXS AS 
(
  select 
    user,
    TRANSACCION,
    linea,
    nombre,
    MAX(trx_timestamp) as FECHA_ULTIMA_TX,
    sum(monto) as MONTO_TOTAL,
    count(*) as N_TX
  from TRANSACCIONES
  group by 1,2,3,4
  having count(*) >= 3
)
,MAYOR_TX_CLIENTE_PRE AS 
(
  select
    user,
    TRANSACCION,
    FECHA_ULTIMA_TX,
    MONTO_TOTAL,
    linea,
    nombre,
    N_TX,
    RANK() OVER (PARTITION BY user ORDER BY MONTO_TOTAL DESC, N_TX DESC, FECHA_ULTIMA_TX DESC) AS rank
  from CONTEO_TXS
)
,MAYOR_TX_CLIENTE as 
(
  select 
    user,
    TRANSACCION,
    linea,
    nombre,
    FECHA_ULTIMA_TX,
    MONTO_TOTAL,
    N_TX,
    ROUND(MONTO_TOTAL/N_TX, 0) as MONTO_PROMEDIO_TX 
  from MAYOR_TX_CLIENTE_PRE
  where rank = 1
)
,TRANSACCIONES_V2 as 
(
  select 
    * 
  from TRANSACCIONES
  where fecha >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 MONTH)
)
,CONTEO_TXS_V2 AS 
(
  select 
    user,
    TRANSACCION,
    MAX(trx_timestamp) as FECHA_ULTIMA_TX_ACTUAL,
    sum(monto) as MONTO_TOTAL_ACTUAL,
    count(*) as N_TX_ACTUAL,
    ROUND((sum(monto)/count(*)),0) as MONTO_PROMEDIO_TX_ACTUAL 
  from TRANSACCIONES_V2
  group by 1,2
)
, TABLA_RESULTADO AS 
(
  select 
    T1.user,
    T1.TRANSACCION,
    T1.linea,
    T1.nombre,
    T1.FECHA_ULTIMA_TX,
    T1.N_TX,
    T1.MONTO_TOTAL,
    T1.MONTO_PROMEDIO_TX,
    case when T2.N_TX_ACTUAL >0 then T2.N_TX_ACTUAL else 0 END N_TX_ACTUAL,
    T2.MONTO_TOTAL_ACTUAL,
    case when T2.MONTO_PROMEDIO_TX_ACTUAL > 0 then T2.MONTO_PROMEDIO_TX_ACTUAL else 0 END MONTO_PROMEDIO_TX_ACTUAL,
    case
      when T2.MONTO_PROMEDIO_TX_ACTUAL >= T1.MONTO_PROMEDIO_TX*0.8 then 'Cliente Monto OK' else 'Cliente Revisar' END CLASIFICACION_CLIENTE,
    case when T2.N_TX_ACTUAL >0 then 'Cliente hizo alguna Tx ultimo mes' else 'Cliente sin Txs ultimo mes' END CLASIFICACION_CLIENTE_HACE_ALGUNA_TX
  from MAYOR_TX_CLIENTE T1 
    left join CONTEO_TXS_V2 T2 on (T1.user = T2.user and T1.TRANSACCION = T2.TRANSACCION)
), tabla_final as (

select distinct user as identity, case when CLASIFICACION_CLIENTE = 'Cliente Monto OK' then 1 else 0 END clasificacion_monto_promedio_cliente
from TABLA_RESULTADO

)

    SELECT
    *
    from tabla_final

);
