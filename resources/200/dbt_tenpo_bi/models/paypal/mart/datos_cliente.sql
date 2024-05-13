{{ 
  config(
    tags=["hourly", "paypal","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT15', 'tenpo-datalake-sandbox')
  ) 
}}

select distinct 
    c.id as id_cliente, 
    correo_usuario, 
    telefono 
from {{source('paypal','pay_cliente')}} c 
join {{source('paypal','entidades')}} e using (rut)
where true
qualify row_number()  over (partition by c.id order by c.fec_hora_actualizacion,e.fecha_actualizacion)=1