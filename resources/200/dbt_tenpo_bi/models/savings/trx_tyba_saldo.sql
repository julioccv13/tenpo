{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}
select 
    t.* except (CODIGO_PARTICIPE),
    u.id as user_id
from {{source('reconciliation','trx_tyba_saldo')}} t
join {{source('tenpo_users','users')}} u on (t.CODIGO_PARTICIPE=u.rut)
where true
qualify ROW_NUMBER() over (partition by ID_PARTICIPE,ID_SERIE,ID_FONDO,FECHA_SALDO order by FECHA_CARGA desc) = 1