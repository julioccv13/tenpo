{{ 
  config(
    tags=["hourly", "paypal","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT15', 'tenpo-datalake-sandbox')
  ) 
}}
with nombres as (
  SELECT
    LAST_VALUE({{target.schema}}.proper_name(nombre)) OVER (PARTITION BY rut ORDER BY fecha_actualizacion) AS nombre,
    rut 
  FROM {{source('paypal','entidades')}}
),
  desuscritos as (
    select 
        {{ hash_sensible_data('email') }} as email,
        date as fecha_desuscrito,
    from {{source('aux_table','unsubscribe_web')}}
    WHERE source = "Paypal"
  )
SELECT distinct
  tipo_usuario,
  cor_mail_envio AS trx_mail,
  tip_trx as type_trx,
  tip_operacion_multicaja as type_account,
  correo_usuario AS user_mail,
  correo_cuenta AS account_mail,
  total_ammount,
  avg_tbt,
  avg_ammount,
  total_trx,
  case 
    when state IS NULL then 'Por migrar'
    when state in (4,7,8,21,22) then 'OB Exitoso'
    else 'Se quedó en OB'
    end as estado_migracion,
  first_trx,
  last_trx,
  fecha_ingreso as date_suscribed,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),last_trx, DAY) as days_inactive,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), fecha_ingreso, DAY) as days_suscribed,
  TIMESTAMP_DIFF(first_trx,fecha_ingreso, DAY) as days_toactivate,
  case 
    when d1.fecha_desuscrito is not null then d1.fecha_desuscrito
    when d2.fecha_desuscrito is not null then d2.fecha_desuscrito
    when d3.fecha_desuscrito is not null then d3.fecha_desuscrito
    end
    as fecha_desuscrito,    
  case 
    when c1.fecha_ini is not null then c1.fecha_ini
    when c2.fecha_ini is not null then c2.fecha_ini
    when c3.fecha_ini is not null then c3.fecha_ini
    end
    as fecha_campana,
  case 
    when c1.campana is not null then c1.campana
    when c2.campana is not null then c2.campana
    when c3.campana is not null then c3.campana
    end
    as nombre_campana,
  if(d1.fecha_desuscrito is not null or d2.fecha_desuscrito is not null or d3.fecha_desuscrito is not null, 1, 0) AS desuscrito,
  if(categoria is null, "B2C",categoria) as categoria,
  nombre
FROM
  {{ref('data_users_info')}}
  left join nombres USING (rut)
  left join desuscritos as d1 ON (d1.email=cor_mail_envio)
  left join desuscritos as d2 ON (d2.email=correo_usuario)
  left join desuscritos as d3 ON (d3.email=correo_cuenta)
  left join {{source('aux_paypal','aux_campanas')}} as c1 ON (c1.correo=cor_mail_envio)
  left join {{source('aux_paypal','aux_campanas')}} as c2 ON (c2.correo=correo_usuario)
  left join {{source('aux_paypal','aux_campanas')}} as c3 ON (c3.correo=correo_cuenta)
