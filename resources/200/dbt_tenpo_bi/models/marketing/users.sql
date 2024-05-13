{{ 
    config(
        materialized='table'
        ,tags=["hourly", "bi"]
        ,project=env_var('DBT_PROJECT19', 'tenpo-datalake-sandbox')
        ,schema='external'
        ,alias='appsflyer_users_dm'
    )
}}


WITH source_data AS (

    SELECT 
      created_at,
      ob_completed_at,
      user,
      state,
      source,
      media_source,
      campaign,
      af_channel,
      event_name,
      event_time,
    FROM {{ ref('appsflyer_weebhooks_data') }}

),

first_cashin AS (

    select
        user,
        fecha,
        trx_timestamp
    from {{ ref('economics') }}
    where linea = 'cash_in'
    qualify row_number() over (partition by user order by trx_timestamp asc) = 1

),


first_trx AS (

    select
      user,
      fecha,
      trx_timestamp
    from {{ ref('economics') }}
    where linea = 'mastercard'
    qualify row_number() over (partition by user order by trx_timestamp asc) = 1


),

first_coupon AS (

    SELECT  
        redeem_date,
        coupon,
        campana,
        id_user user,
    from {{ ref('coupons') }}
    WHERE objective = 'Adquisición'
    AND confirmed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY redeem_date ASC) = 1    

)


SELECT 
    source_data.user,
    state,
    source_data.ob_completed_at,
    first_cashin.fecha as fecha_fci,
    first_trx.fecha as fecha_ftrx,
    date_trunc(date(ob_completed_at), MONTH) camada,
    dic.channel_grouping,
    source_data.media_source,
    source_data.campaign,
    source_data.source,
    first_coupon.coupon
FROM source_data
LEFT JOIN first_cashin USING (user)
LEFT JOIN first_trx USING (user)
LEFT JOIN first_coupon USING (user)
LEFT JOIN {{ source('diccionario_media_source', 'channel_grouping_dbt') }} dic ON dic.media_source = source_data.media_source
ORDER BY ob_completed_at DESC
