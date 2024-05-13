WITH CLIENTES_INVITA_GANA AS 
(
  select distinct user
  from `{project_source_1}.economics.economics`  
  where linea = 'reward' and comercio in ('Promoción invita y gana','Premio por Invita y Gana','Recompensa por invitacion a un amigo','Invita y Gana')
)
,SEGMENTO_2 as 
(
  select distinct 'i' AS type, user AS identity
  from CLIENTES_INVITA_GANA
)

select type, identity from SEGMENTO_2