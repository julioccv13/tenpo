{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


SELECT  fecha,
          COUNTIF(motor='desconocido') desconocido,
          COUNTIF(motor='invita y gana') invita_y_gana,
          COUNTIF(motor='organic') organic,
          COUNTIF(motor='paid media') paid_media,
          COUNTIF(motor='partnership') partnership,
          COUNTIF(motor='restricted_channel') restricted_channel,
          COUNTIF(motor='xsell_paypal') xsell_paypal,
          COUNTIF(motor='xsell_recargas') xsell_recargas 
FROM {{ref('onboardings_motores')}}
GROUP BY fecha ORDER BY fecha DESC
