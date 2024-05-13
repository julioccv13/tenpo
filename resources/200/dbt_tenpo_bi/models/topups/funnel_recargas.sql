{{ config(materialized='table') }}

select * from {{ref('funnel_sitios_old')}}
UNION ALL
select * from {{ref('funnel_nuevo_rf')}}