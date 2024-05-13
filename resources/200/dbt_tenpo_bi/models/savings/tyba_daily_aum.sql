{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

SELECT 
  *
FROM (
  select 
    CAST(sum(SALDO_CUOTAS) as numeric) AS aum,
    case 
      when ID_FONDO = 3209 then "Liquidez"
      when ID_FONDO = 3191 then "Deuda_360"
      when ID_FONDO = 3016 then "Light_Reggae"
      when ID_FONDO = 3018 then "Power_Pop"
      when ID_FONDO = 3024 then "Brave_Disco"
      when ID_FONDO = 3020 then "Risky_Rock"
      when ID_FONDO = 3022 then "Heavy_Metal"
      end as nombre_fondo, 
    date(FECHA_SALDO) as fecha,
    sum(CAST(sum(SALDO_CUOTAS) as numeric)) over (partition by FECHA_SALDO) AS total,
    count(distinct user_id) over (partition by FECHA_SALDO) AS usuarios,
  FROM {{ref('trx_tyba_saldo')}}
  WHERE TRUE
  group by FECHA_SALDO,ID_FONDO,user_id
  order by FECHA_SALDO asc
)  
PIVOT
(
  SUM(aum) AS aum_fm
  FOR nombre_fondo in ('Liquidez',
  'Deuda_360',
  'Light_Reggae',
  'Power_Pop',
  'Brave_Disco',
  'Risky_Rock',
  'Heavy_Metal'
  )
)
ORDER BY FECHA desc