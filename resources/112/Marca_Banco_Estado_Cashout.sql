DROP TABLE IF EXISTS `${project_target}.tmp.Marca_Banco_Estado_Cashout_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Marca_Banco_Estado_Cashout_{{ds_nodash}}` AS (
WITH 
T1 as (select distinct user as identity from `${project_source_1}.bancarization.cca_cico_tef`
where tipo_trx = 'cashout'
and cico_banco_tienda_destino = 'BANCO ESTADO'
and date(ts_trx) >= date_add(CURRENT_DATE(),INTERVAL -7 DAY))
,tabla_final as (select identity, 1 as M_Banco_Estado from T1)

SELECT
*
from tabla_final
);
