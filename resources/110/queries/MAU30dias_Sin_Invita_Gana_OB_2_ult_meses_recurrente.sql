WITH CLIENTES_INVITA_GANA AS 
(
  select distinct user
  from `{project_source_1}.economics.economics`  
  where linea = 'reward' and comercio in ('Promoción invita y gana','Premio por Invita y Gana','Recompensa por invitacion a un amigo','Invita y Gana')
)
,CLIENTES_MAU_30_DIAS AS 
(
select distinct user
from `{project_source_1}.economics.economics`  
where linea not in ('reward','aum_savings','aum_tyba') and fecha >= (DATE_ADD(CURRENT_DATE(), INTERVAL -1 MONTH))
)
,TABLA_CLIENTES AS
(
  select 
  id as ID,
  nationality,
  case 
    when nationality in ('CHILE','CHILENA','CHILENO','CHL','Chile','chile','Chileno','chileno') then 'chile' else 'extranjero' end NACIONALIDAD,
  case
    when gender in ('MALE','MASCULINO','Male','Masculino') then 'MASCULINO'
    when gender in ('FEMENINO','Female','Femenino') then 'FEMENINO'
    when IFNULL(gender,'S/I') = 'S/I' then 'S/I'
    when gender in ('Sin informacion','Sin información') then 'S/I' else gender END GENERO,
  date_of_birth,
  IFNULL( 
  (DATE_DIFF(CURRENT_DATE(), EXTRACT(DATE from date_of_birth),YEAR)
  - IF( (EXTRACT(MONTH FROM EXTRACT(DATE from date_of_birth))*100 + EXTRACT(DAY FROM EXTRACT(DATE from date_of_birth))) > (EXTRACT(MONTH FROM CURRENT_DATE())*100 + EXTRACT(DAY FROM CURRENT_DATE())),1,0)
  )
  ,0) AS EDAD,
  (EXTRACT(YEAR FROM EXTRACT(DATE FROM ob_completed_at))*100 + EXTRACT(MONTH FROM EXTRACT(DATE FROM ob_completed_at))) as CAMADA_OB,
  EXTRACT(DATE FROM ob_completed_at) as FECHA_OB
  from `{project_source_3}.users.users` 
  where state = 4
)
,SEGMENTO_1 as 
(
  select distinct 'i' AS type, user AS identity
  from CLIENTES_MAU_30_DIAS
  where 
    user not in (select user from CLIENTES_INVITA_GANA)
    and user in (select distinct ID from TABLA_CLIENTES where EDAD between 18 and 35)
    and user in (select distinct ID from TABLA_CLIENTES where FECHA_OB >= DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH))
)

select distinct type, identity from SEGMENTO_1