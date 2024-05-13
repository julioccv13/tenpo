{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


with table1a as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'cash_in'
    group by 1 order by 1 desc
), table2a as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event='Carga plata online CCA'
),
table3a as (
SELECT 'Carga plata online CCA' AS event,*
from table1a LEFT JOIN table2a using(fecha)
order by fecha DESC),

----------------------------------------------------------------------------
table1b as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'cash_out' and nombre ='Cashout TEF'
    group by 1 order by 1 desc
), table2b as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event='Retira plata online' 
), 
table3b as (
SELECT 'Retira plata online' as event,*
from table1b LEFT JOIN table2b using(fecha)
order by fecha DESC),

----------------------------------------------------------------------------
table1c as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'cash_out' and nombre <> 'Cashout TEF'
    group by 1 order by 1 desc
), table2c as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event like 'Retira plata f%'
),
table3c as (
SELECT 'Retira plata físico' as event,*
from table1c LEFT JOIN table2c using(fecha)
order by fecha DESC),


----------------------------------------------------------------------------
table1d as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'top_ups'
    group by 1 order by 1 desc
), table2d as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event='Recarga exitosa'
),
table3d as (
SELECT 'Recarga exitosa' as event,*
from table1d LEFT JOIN table2d using(fecha)
order by fecha DESC),


----------------------------------------------------------------------------
table1e as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea like '%mastercard%'
    group by 1 order by 1 desc
), table2e as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event = 'Compra autorizada'
),
table3e as (
SELECT 'Compra autorizada' as event,*
from table1e LEFT JOIN table2e using(fecha)
order by fecha DESC),



----------------------------------------------------------------------------
table1f as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'cash_out_savings'
    group by 1 order by 1 desc
), table2f as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event = 'Confirma rescate'
),
table3f as (
SELECT 'Confirma rescate' as event,*
from table1f LEFT JOIN table2f using(fecha)
order by fecha DESC),



----------------------------------------------------------------------------
table1g as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'cash_in_savings'
    group by 1 order by 1 desc
), table2g as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event like 'Confirmar inversi%'
),
table3g as (
SELECT 'Confirmar inversión' as event,*
from table1g LEFT JOIN table2g using(fecha)
order by fecha DESC),


----------------------------------------------------------------------------
table1h as (
    SELECT
        fecha, -- hay que transformar la columna DATE para comparar igual con la economics con la formula FORMAT_DATE
        count(distinct trx_id) transacciones
    FROM {{ref('economics')}}
    where linea = 'crossborder'
    group by 1 order by 1 desc
), table2h as (
SELECT count, CAST(PARSE_DATE("%Y%m%d", CAST(date AS STRING)) AS DATE) as fecha FROM {{source('clevertap','trends')}}
where event = 'Envio Remesa Existosa'
),
table3h as (
SELECT  'Envio Remesa Existosa' as event,*
from table1h LEFT JOIN table2h using(fecha)
order by fecha DESC),

union_table as (
select * from table3a
union all 
select * from table3b
union all 
select * from table3c
union all 
select * from table3d
union all 
select * from table3e
union all 
select * from table3f
union all 
select * from table3g
union all 
select * from table3h)

select * from  union_table
where fecha >= '2022-01-01'
order by event desc, fecha desc

