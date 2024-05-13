{{ config(materialized='table') }}

with data as (
    SELECT DISTINCT
        rut,
        FIRST_VALUE(t1.fec_hora_ingreso) OVER (partition by rut order by t1.fec_hora_ingreso)  as ts_creacion_paypal,
        LAST_VALUE(t2.correo_cuenta) OVER (PARTITION BY rut order by t1.fec_ultimo_login_exitoso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as email,
        LAST_VALUE(t1.ultimo_servicio) OVER (PARTITION BY rut order by t1.fec_ultimo_login_exitoso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as last_serv_paypal,
        LAST_VALUE(t1.fec_ultimo_login_exitoso) OVER (PARTITION BY rut order by t1.fec_ultimo_login_exitoso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as last_login,
        LAST_VALUE(t1.tipo_usuario) OVER (PARTITION BY rut order by t1.fec_ultimo_login_exitoso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as tipo_usuario,
        LAST_VALUE(t3.fec_fechahora_envio) OVER (PARTITION BY rut order by t3.fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as last_trx_pp,
        FROM
        {{ source('paypal', 'pay_cliente') }} as t1
        JOIN 
        {{ source('paypal', 'pay_cuenta') }} as t2
        ON t1.id = t2.id_cliente 
        LEFT JOIN
        {{ source('paypal', 'pay_transaccion') }} t3 ON t2.id = t3.id_cuenta
        where t2.tip_operacion_multicaja = t1.ultimo_servicio
)

select
    rut -- TODO: pasar a tributary_identifier y mejor usar id_cliente_paypal
    ,ts_creacion_paypal
    ,{{ hash_sensible_data('email') }} as email
    ,last_serv_paypal
    ,last_login
    ,tipo_usuario
    ,last_trx_pp

from data