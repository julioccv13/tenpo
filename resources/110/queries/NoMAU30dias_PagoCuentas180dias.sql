WITH CLIENTES_TOP_UPS AS 
(
  select distinct user
  from `{project_source_1}.economics.economics`  
  where linea = 'top_ups' and fecha >= (DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH))
)
,CLIENTES_CUENTAS_BASICAS AS 
(
  select distinct user
  from `{project_source_1}.economics.economics`  
  where linea = 'utility_payments' and fecha >= (DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH))
)
,CLIENTES_MAU_30_DIAS AS 
(
select distinct user
from `{project_source_1}.economics.economics`  
where linea not in ('reward','aum_savings','aum_tyba') and fecha >= (DATE_ADD(CURRENT_DATE(), INTERVAL -1 MONTH))
)

,SEGMENTO_1 as 
(
  select distinct 'i' AS type, user AS identity
  from CLIENTES_CUENTAS_BASICAS
  where 
    user not in (select user from CLIENTES_MAU_30_DIAS) 
)

select distinct type, identity from SEGMENTO_1