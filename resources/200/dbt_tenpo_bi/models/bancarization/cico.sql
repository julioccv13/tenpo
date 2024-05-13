{{ 
  config(
    materialized='ephemeral', 
  ) 
}}

select * from {{ ref('cca_cico_tef') }}
union all
select * from {{ ref('physical_cico') }}