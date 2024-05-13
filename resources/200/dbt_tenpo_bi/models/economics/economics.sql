/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}
{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "linea",
    partition_by = {'field': 'fecha', 'data_type': 'date'},
    incremental_strategy = 'insert_overwrite'
  ) 
}}

WITH 
  economics_app AS (
    SELECT  fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('cashin') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('cashout') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('p2p_enviado') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('p2p_recibido') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('topups') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('utility_payments') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('mastercard_physical') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('mastercard_virtual') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('paypal') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('rewards') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('cashin_savings') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('cashout_savings') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('aum_savings') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('crossborder') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('tyba_economics') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('aum_tyba') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('balance') }}
    UNION ALL
    SELECT fecha ,trx_timestamp ,nombre ,monto ,trx_id ,user ,linea ,canal ,TRIM(comercio) comercio ,id_comercio ,actividad_cd ,actividad_nac_cd ,codact, tipofac
    FROM {{ ref('pfm') }}
    )
    
   SELECT DISTINCT
     a.user
     ,{{ hash_sensible_data('c.email') }} as email
     ,a.* EXCEPT(user)
     ,concat(a.linea, '_', trx_id, '_', trx_timestamp) as economics_trx_id
     ,DATETIME(trx_timestamp,  "America/Santiago") as dt_trx_chile
     ,b.* EXCEPT(linea, comercio)
     ,cat_tenpo1
     ,cat_tenpo2
     ,d.tipo tipo_trx
     ,d.glosa
     ,case 
        when nombre="QR Payment" then "Redpay"
        when nombre="Cashin Pago Nomina" then "Payroll"
        --when nombre like "Cashout ATM%" then "Cashout ATM"
        else a.linea
      end as nueva_linea
   FROM economics_app a
   LEFT JOIN {{ ref('rubros_comercios')}} b ON a.linea = b.linea and a.comercio = b.comercio
   LEFT JOIN {{source('aux_table','mcc_codes')}} ON CAST(codact AS INT64) = CAST(MCC  as INT64)
   LEFT JOIN (SELECT * FROM {{source('prepago','tipos_factura')}} WHERE indnorcor = 0) d  USING(tipofac)
   JOIN {{ ref('users_tenpo')}} c ON c.id = a.user
   WHERE fecha <= CURRENT_DATE("America/Santiago")
   QUALIFY row_number() over (partition by concat(a.linea, '_', trx_id, '_', trx_timestamp) order by trx_timestamp desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}

/*
    Uncomment the line below to remove records with null id values
*/

-- where id is not null
