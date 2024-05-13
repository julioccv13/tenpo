DROP TABLE IF EXISTS `tenpo-bi.tmp.Saldo_Diario_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.Saldo_Diario_{{ds_nodash}}` AS (
with Tabla_Final as (
select     user as identity , cast(saldo_dia as int) saldo_dia
  FROM `tenpo-bi-prod.balance.daily_balance`
  where fecha = (select max(fecha) from `tenpo-bi-prod.balance.daily_balance`)
  )

select distinct
      T1.id as identity,
      IFNULL(T2.saldo_dia,0) as saldo_dia
from `tenpo-bi-prod.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.identity
where status_onboarding = 'completo'
);
