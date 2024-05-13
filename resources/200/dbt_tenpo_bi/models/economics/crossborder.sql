  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}

{{ 
  config(
    materialized='ephemeral',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    enabled=True
  ) 
}}



with target as (
 SELECT DISTINCT
            DATE(trx.created_at , "UTC") as fecha ,
            trx.created_at as trx_timestamp,
            -- TODO: Pasar a tabla parametrica
            case 
                when destination_currency = "ar_ars" then "Argentina"
                when destination_currency = "uy_uyu" then "Uruguay"
                when destination_currency = "py_pyg" then "Paraguay"
                when destination_currency = "us_usd" then "EE.UU"
                when destination_currency = "mx_mxn" then "México"
                when destination_currency = "br_brl" then "Brasil"
                when destination_currency = "co_cop" then "Colombia"
                when destination_currency = "pe_pen" then "Perú"
                when destination_currency = "bo_bob" then "Bolivia"
                when destination_currency = "ve_vef" then "Venezuela"
                else null
            end as nombre,
            cast(trx.origin_amount as FLOAT64) as monto,
            cast(trx.id as string) as trx_id,
            trx.user_id as user,
            'crossborder' as linea,
            'app' as canal,
            concat('Dest.: ',destination_account_id) as comercio,
            CAST(null as STRING) as id_comercio,
            CAST(null as STRING) as actividad_cd,
            CAST(null as STRING) as actividad_nac_cd,
            CAST(null AS FLOAT64) as codact ,
            null as tipofac,
            row_number() over (partition by CAST(trx.id AS STRING) order by trx.updated_at desc) as row_no_actualizacion 
        FROM {{source('crossborder','transaction')}} trx
        WHERE state IN ("ACCEPTED","IN_PROGRESS", "SUCCESSFUL")
)

SELECT
* EXCEPT(row_no_actualizacion)
FROM target
where row_no_actualizacion = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
