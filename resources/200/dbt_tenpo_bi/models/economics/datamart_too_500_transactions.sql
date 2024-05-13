  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}

{{ 
    config(
        materialized='table'
        ,tags=["hourly", "bi"]
        ,project=env_var('DBT_PROJECT24', 'tenpo-datalake-sandbox')
        ,schema='insumos_jarvis'
        ,alias='transactions_user_info'
        ,incremental_strategy = 'insert_overwrite'
    )
}}

SELECT
DISTINCT A.FECHA
,EXTRACT(DAYOFWEEK FROM A.FECHA) DIA_DE_SEMANA
,LTRIM(RIGHT(A.ID_COMERCIO,8), '0') ID_COMERCIO
,A.NOMBRE
,A.COMERCIO
,A.RUBRO_RECOD RUBRO
,A.comercio_recod
,A.cat_tenpo1
,A.cat_tenpo2
,A.tipo_trx
,A.glosa
,A.MONTO
,A.TRX_ID
,A.USER
,C.RUT
,C.PHONE
,C.FIRST_NAME
,C.LAST_NAME
,C.EMAIL

FROM {{ref('economics')}} A
LEFT JOIN  {{source('tenpo_users','users')}} C
ON A.USER = C.ID
WHERE TRUE    
and A.NOMBRE NOT LIKE "%Devolución%"
AND LOWER(LINEA) LIKE '%master%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY A.TRX_ID ORDER BY FECHA DESC) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}