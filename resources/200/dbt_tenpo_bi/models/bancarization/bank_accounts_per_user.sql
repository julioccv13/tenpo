{{ 
  config(
    materialized='table', 
  ) 
}}

with data as (
        select 
            b.user
            ,tipo_trx
            ,cico_banco_tienda_destino as banco
            ,cuenta_destino as cuenta
            ,case when (cico_banco_tienda_destino = 'BANCO SANTANDER/BANEFE' AND LEFT(LTRIM(cuenta_destino, '0'), 3) = '710') then 'Cuenta Prepago'
                  when (cico_banco_tienda_destino = 'BANCO RIPLEY' AND LEFT(LTRIM(cuenta_destino, '0'), 3) = '999') then 'Cuenta Prepago'
                  when (cico_banco_tienda_destino = 'BCI/TBANC/NOVA' AND LEFT(LTRIM(cuenta_destino, '0'), 3) = '777') then 'Cuenta Prepago'
                  when cico_banco_tienda_destino = 'LOS HEROES' then 'Cuenta Prepago'
                  when cico_banco_tienda_destino = 'COOPEUCH' then 'Cuenta Prepago'
                  else destinationaccounttype end as tipo_cuenta
            ,ts_trx
            
        FROM {{ source('tenpo_users', 'users') }} a
        LEFT JOIN {{ ref('cca_cico_tef') }} b
        ON a.id = b.user
        where trx_mismo_rut and tipo_trx = 'cashout'
        and canal = 'tef_cca'

        union all 

        select 
            b.user
            ,tipo_trx
            ,cico_banco_tienda_origen as banco
            ,cuenta_origen as cuenta
            ,case when (cico_banco_tienda_origen = 'BANCO SANTANDER/BANEFE' AND LEFT(LTRIM(cuenta_origen, '0'), 3) = '710') then 'Cuenta Prepago'
                  when (cico_banco_tienda_origen = 'BANCO RIPLEY' AND LEFT(LTRIM(cuenta_origen, '0'), 3) = '999') then 'Cuenta Prepago'
                  when (cico_banco_tienda_origen = 'BCI/TBANC/NOVA' AND LEFT(LTRIM(cuenta_origen, '0'), 3) = '777') then 'Cuenta Prepago'
                  when cico_banco_tienda_origen = 'LOS HEROES' then 'Cuenta Prepago'
                  when cico_banco_tienda_origen = 'COOPEUCH' then 'Cuenta Prepago'
                  else 'Desconocido' end as tipo_cuenta
            ,ts_trx
            
        FROM {{ source('tenpo_users', 'users') }} a
        LEFT JOIN {{ ref('cca_cico_tef') }} b
        ON a.id = b.user
        where trx_mismo_rut and tipo_trx = 'cashin'
        and canal = 'tef_cca'
)

select 
    user
    ,banco
    ,cuenta
    ,max(tipo_cuenta) as tipo_cuenta
    ,min(ts_trx) as first_trx_date
from data
group by 1,2,3