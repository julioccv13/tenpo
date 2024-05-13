WITH LISTADO_TXS as 
(
    select
    *
    from `{project_source_1}.prepago.notifications_history`
    where f_aprobada is false
    and descripcion_sia = 'IMPORTESUPERALIMITE' and tipo_tx = 'suscripción'
    and (EXTRACT(YEAR from trx_timestamp)*100 + EXTRACT(MONTH from trx_timestamp)) >= (EXTRACT(YEAR from DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH))*100 + EXTRACT(MONTH from DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH) ))
    and (EXTRACT(YEAR from trx_timestamp)*100 + EXTRACT(MONTH from trx_timestamp)) <= (EXTRACT(YEAR from DATE_ADD(CURRENT_DATE(), INTERVAL -1 MONTH))*100 + EXTRACT(MONTH from DATE_ADD(CURRENT_DATE(), INTERVAL -1 MONTH) ))
)
, DIA_POR_CLIENTE as 
(
    select
    user,
    EXTRACT(DAY from trx_timestamp) as DIA,
    count(*) as N_TXS
    from LISTADO_TXS
    group by 1,2
)
, MAX_N_TXS as 
(
    select user, max(N_TXS) as N_MAXIMO
    from DIA_POR_CLIENTE T1
    group by 1
)
, DIA_MAS_TXS as 
(
    select  
        T1.user, 
        min(T1.DIA) as DIA_TXS_RECHAZADAS,
        case 
            when min(T1.DIA) = 1 then 29
            when min(T1.DIA) = 2 then 30 else (min(T1.DIA) - 2) end as DIA_RECORDAR_SALDO
    from DIA_POR_CLIENTE T1 
        left join MAX_N_TXS T2 on T1.user = T2.user and T1.N_TXS = T2.N_MAXIMO
    where T2.user is not null
    group by 1
)
select distinct 'i' as type, user as identity from DIA_MAS_TXS where DIA_RECORDAR_SALDO = EXTRACT(DAY from CURRENT_DATE())