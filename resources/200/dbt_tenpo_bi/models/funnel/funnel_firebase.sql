{% set today = modules.datetime.date.today() %}
{% set yesterday_iso = (today - modules.datetime.timedelta(2)).strftime("%Y%m%d") %}
{% set days_ago_iso = (today - modules.datetime.timedelta(3)).strftime("%Y%m%d") %}


{{ 
  config(
    tags=["hourly", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}


WITH 
    funnel_paypal_firebase as(
        SELECT
            event_date,
            parse_date("%Y%m%d", event_date) AS fecha,
            case 
                when event_name = 'pantalla_retiro_PP' then '1. Home'
                when event_name = 'Retiro_en_curso_PP'  then '2. Retiro en Curso'
                when event_name = 'Retiro_exitoso_PP' then '3. Retiro Exitoso'
            end as paso,
            'paypal' as funnel,
            user_id,
{% if is_incremental() %}
    FROM (
        SELECT * FROM {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+yesterday_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+days_ago_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_intraday_*`' }}
    )
{% else %}
    FROM {{source('analytics_185654164','events')}} 
{% endif %}
        where event_name in (
            'pantalla_retiro_PP',
            'Retiro_en_curso_PP',
            'Retiro_exitoso_PP')

            ),
    funnel_crossborder_firebase as(
        SELECT
             event_date,
             parse_date("%Y%m%d", event_date) AS fecha,
            case 
                when event_name = 'simulador_CB' then '1. Simular'
                when event_name = 'lista_de_contactos_CB' or event_name = 'lista_de_contactos_seleccion_CB' then '2. Seleccionar destinatario'
                when event_name = 'motivo_transaccion_CB' then '3. Indicación de Motivo'
                when event_name = 'modal_confiracion_transaccion_CB'  or event_name = 'confirmation_transaction_CB' then '4. Confirmación de Transacción'
                when event_name = 'transaccion_exitosa_CB' or event_name = 'comprobante_transaccion_CB' or event_name = 'exito_envio_CB' then '5. Exito'
            end as paso,
            'crossborder' as funnel,
            user_id,
{% if is_incremental() %}
    FROM (
        SELECT * FROM {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+yesterday_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+days_ago_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_intraday_*`' }}
    )
{% else %}
    FROM {{source('analytics_185654164','events')}} 
{% endif %}
        where event_name in (
            'simulador_CB',
            'lista_de_contactos_CB',
            'lista_de_contactos_seleccion_CB',
            'motivo_transaccion_CB',
            'modal_confiracion_transaccion_CB',
            'confirmation_transaction_CB',
            'transaccion_exitosa_CB',
            'comprobante_transaccion_CB',
            'exito_envio_CB')

            ),
    funnel_topups_firebase as(

        SELECT
            event_date,
            parse_date("%Y%m%d", event_date) AS fecha,
            CASE WHEN t.param.value.string_value = 'recargas_home' THEN 'Menú recargas' 
             WHEN t.param.value.string_value = 'recargas_form' THEN 'Llena formulario' 
             WHEN t.param.value.string_value = 'recargas_receipt' THEN 'Recarga exitosa' 
             END AS paso,
            'top_ups' as funnel,
            user_id
        FROM(
                SELECT 
                     event_date, 
                     parse_date("%Y%m%d", event_date) AS fecha,
                    user_id, 
                    event_name, 
                    param
{% if is_incremental() %}
    FROM (
        SELECT * FROM {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+yesterday_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+days_ago_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_intraday_*`' }}
    )
{% else %}
    FROM {{source('analytics_185654164','events')}} 
{% endif %},
        UNNEST(event_params) AS param
        WHERE 
            event_name = 'screen_view'
            AND  param.key = "firebase_screen"
            AND param.value.string_value in ('recargas_home', 'recargas_form', 'recargas_receipt')

            )  t 
            GROUP BY event_date, user_id, event_name, param.value.string_value

),

    funnel_pdc_firebase as(
        SELECT
            event_date,
            parse_date("%Y%m%d", event_date) AS fecha,
            CASE WHEN t.param.value.string_value = 'pdc_home' THEN 'Menú pdc' 
               WHEN t.param.value.string_value = 'pdc_category' THEN 'Selecciona categoría' 
               WHEN t.param.value.string_value = 'pdc_utility' THEN 'Selecciona cuenta' 
               WHEN t.param.value.string_value = 'pdc_identifier' THEN 'Ingresa Id' 
               WHEN t.param.value.string_value = 'pdc_debts' THEN 'Confirma' 
               WHEN t.param.value.string_value = 'pdc_receipt' THEN 'Pdc Exitoso'
               END AS paso,
            'pdc' as funnel,
            user_id
        FROM(
            SELECT 
                 event_date, 
                user_id, 
                event_name, 
                param
{% if is_incremental() %}
    FROM (
        SELECT * FROM {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+yesterday_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+days_ago_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_intraday_*`' }}
    )
{% else %}
    FROM {{source('analytics_185654164','events')}} 
{% endif %}
,
        UNNEST(event_params) AS param
        WHERE 
            event_name = 'screen_view'
            AND  param.key = "firebase_screen"
            AND param.value.string_value in ('pdc_home', 'pdc_category', 'pdc_utility', 'pdc_identifier', 'pdc_debts', 'pdc_receipt')
            ) t 
        GROUP BY event_date, user_id, event_name, param.value.string_value
            ), 

    funnel_p2p_firebase as (
        SELECT
            event_date,
            parse_date("%Y%m%d", event_date) AS fecha,
            CASE WHEN t.event_name = 'p2p_home' THEN 'Menú p2p' 
             WHEN t.event_name = 'p2p_send' THEN 'Selecciona pago'
             WHEN t.event_name = 'p2p_receive' THEN 'Selecciona cobro'
             WHEN t.event_name in (  'p2p_add_contact_send' , 'p2p_add_contact_receive' , 'p2p_add_amount_send', 'p2p_add_amount_receive')THEN 'Ingresa información' 
             WHEN t.event_name = 'p2p_send_succeded' THEN 'Pago p2p exitoso' 
             WHEN t.event_name = 'p2p_receive_succeded' THEN 'Envío cobro p2p exitoso'
             END AS paso,
            CASE WHEN t.event_name in ('p2p_home', 'p2p_send', 'p2p_add_contact_send', 'p2p_add_amount_send', 'p2p_send_succeded') THEN 'pago' 
             WHEN t.event_name in ('p2p_receive', 'p2p_add_contact_receive', 'p2p_add_amount_receive','p2p_receive_succeded') THEN 'cobro' 
             END as funnel,
            user_id
        FROM(
            SELECT 
                 event_date, 
                 parse_date("%Y%m%d", event_date) AS fecha,
                user_id, 
                event_name, 
                param
{% if is_incremental() %}
    FROM (
        SELECT * FROM {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+yesterday_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_'+days_ago_iso+'`' }}
        UNION ALL
        SELECT * FROM  {{ '`'+env_var('DBT_PROJECT27', 'tenpo-datalake-sandbox')+'.analytics_185654164.events_intraday_*`' }}
    )
{% else %}
    FROM {{source('analytics_185654164','events')}} 
{% endif %},
            UNNEST(event_params) AS param
            WHERE 
                event_name in ('p2p_home', 'p2p_send', 'p2p_add_contact_send', 'p2p_add_amount_send', 'p2p_send_succeded', 'p2p_receive', 'p2p_add_contact_receive', 'p2p_add_amount_receive','p2p_receive_succeded')
                AND param.key = "firebase_screen"
        ) t
        WHERE TRUE

        GROUP BY 
            event_date, user_id, event_name
        HAVING 
            paso  is not null AND user_id is not null
)
select 
* 
from funnel_crossborder_firebase

UNION ALL

select 
* 
from funnel_paypal_firebase

UNION ALL

select 
* 
from funnel_p2p_firebase 

union all

select
 * 
from funnel_pdc_firebase

union all

select
 * 
from funnel_topups_firebase 