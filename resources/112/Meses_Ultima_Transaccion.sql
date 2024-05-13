DROP TABLE IF EXISTS `${project_target}.tmp.Meses_Ultima_Transaccion_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Meses_Ultima_Transaccion_{{ds_nodash}}` AS (
with 
DIF_MES_OB as 
(
  select 
    T1.id as user
    ,DATE_DIFF(DATE(EXTRACT(YEAR from CURRENT_DATE()), EXTRACT(MONTH from CURRENT_DATE()), 1),DATE(EXTRACT(YEAR from date(ob_completed_at)), EXTRACT(MONTH from date(ob_completed_at)), 1), MONTH) as DIF
  from `${project_source_1}.users.users_tenpo` T1
  where status_onboarding = 'completo'
)
,DEFINICION_OB as 
(
  select
    user
    ,case 
      when DIF = 0 then 'M0' 
      when DIF = 1 then 'M1'
      when DIF = 2 then 'M2'
      when DIF = 3 then 'M3'
      when DIF = 4 then 'M4'
      when DIF = 5 then 'M5'
      when DIF >= 6 then 'M6+' else 'Revisar Caso' end as Mes_OB
  from DIF_MES_OB  
)
,ULTIMA_TX as 
(
  SELECT 
    user,
    CAST(max(fecha) as date) as ultima_tx_mau
  FROM `${project_source_1}.economics.economics`
  WHERE
      linea in ('p2p','p2p_received','cash_in_savings','crossborder','investment_tyba','mastercard','mastercard_physical','paypal_abonos','paypal','top_ups','utility_payments','withdrawal_tyba','cash_in','aum_tyba','aum_savings','cash_out_savings','cash_out')
      AND linea NOT LIKE '%PFM%'
      AND nombre NOT LIKE '%Home%'
      AND linea != 'Saldo'
      AND linea != 'saldo'  
      and lower(nombre) not like '%devoluc%'
      AND linea <> 'reward'
  group by 1
)
, DIF_MES as
(
  select 
    user
    ,ultima_tx_mau
    ,DATE_DIFF(DATE(EXTRACT(YEAR from CURRENT_DATE()), EXTRACT(MONTH from CURRENT_DATE()), 1),DATE(EXTRACT(YEAR from ultima_tx_mau), EXTRACT(MONTH from ultima_tx_mau), 1), MONTH) as DIF
  from ULTIMA_TX
)
, DEFINICION as
(
  select
    user
    ,ultima_tx_mau
    ,DIF
    ,case 
      when DIF = 0 then 'MAU' 
      when DIF = 1 then '1M'
      when DIF = 2 then '2M'
      when DIF = 3 then '3M'
      when DIF = 4 then '4M'
      when DIF = 5 then '5M'
      when DIF >= 6 then '6M+' else 'Revisar Caso' end as Meses_Ultima_Transaccion
  from DIF_MES
)
, Tabla_Final as 
(
  select
        T1.id as identity
        ,case when T2.Meses_Ultima_Transaccion is null then 'Nunca_Mau' else T2.Meses_Ultima_Transaccion end as Meses_Ultima_Transaccion
        ,case when T3.Mes_OB is null then 'S/I' else T3.Mes_OB end as Mes_OB
        ,T4.mau_type as Tipo_Ultima_vez_MAU
  from `${project_source_1}.users.users_tenpo` T1
    left join DEFINICION T2 on T1.id = T2.user
    left join DEFINICION_OB T3 on T1.id = T3.user
    left join `${project_source_1}.users.demographics` T4 on T1.id = T4.id_usuario
  where status_onboarding = 'completo'
)

SELECT
*
from Tabla_Final
);
