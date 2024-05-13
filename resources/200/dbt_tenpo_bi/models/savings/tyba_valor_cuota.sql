{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

with fondos as (
    SELECT DISTINCT
        ID_FONDO as id_fondo,
        ID_SERIE as id_serie,
    FROM {{source('reconciliation','trx_tyba_cuota')}}
),
fechas as (
    SELECT day as fecha
    FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE('2021-11-09'), CURRENT_DATE(), INTERVAL 1 DAY)
    ) AS day
),
datos as(
    SELECT DISTINCT
        FECHA AS fecha,
        ID_FONDO as id_fondo,
        ID_SERIE as id_serie,
        VALOR_CUOTA as valor_cuota
    FROM {{source('reconciliation','trx_tyba_cuota')}}
    where true
    qualify row_number() over (partition by ID_FONDO,ID_SERIE,FECHA order by FECHA_CARGA desc)=1
    order by fecha,id_fondo,id_serie
)
select 
    * except (valor_cuota),
    case 
      when id_fondo = 3209 then "Liquidez"
      when id_fondo = 3191 then "Deuda_360"
      when id_fondo = 3016 then "Light_Reggae"
      when id_fondo = 3018 then "Power_Pop"
      when id_fondo = 3024 then "Brave_Disco"
      when id_fondo = 3020 then "Risky_Rock"
      when id_fondo = 3022 then "Heavy_Metal"
      end as nombre_fondo, 
    last_value(valor_cuota ignore nulls) over (partition by id_fondo order by fecha rows between unbounded preceding and current row) as valor_cuota
from fechas 
    join fondos on (1=1)
    left join datos using (fecha,id_fondo,id_serie)
order by fecha, id_fondo