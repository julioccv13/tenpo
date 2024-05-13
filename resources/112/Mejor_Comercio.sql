DROP TABLE IF EXISTS `${project_target}.tmp.Mejor_Comercio_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Mejor_Comercio_{{ds_nodash}}` AS (
with a as(
select eco.comercio_recod as NOM_Comercio,
         count(*) N_TRX,
         row_number() over (order by count(*) desc) ID
from  `${project_source_1}.economics.economics`  as eco
        where eco.fecha >= date_add(CURRENT_DATE(),INTERVAL -3 MONTH)
        and linea in ('mastercard',' mastercard_physical')
        and tipo_trx = 'Compra'
        and fecha >= date_add(CURRENT_DATE(),INTERVAL -3 MONTH)
        and eco.comercio_recod not in ('paypal','hdi seguros','kushki','facebook','pagos flow','mercado pago sa','ebanx','onlyfans','webpay','pago online', 'municipalidad','verificacion ks','binance','alps spa','otros marketplaces','pago facil','payu','pass line ltda','movired','recorrido','recarga pagoya 1','tinder','soap bci seguros web')
        and eco.rubro_recod not in ('Pasarelas de pago','Transporte','Pago de Cuentas','Otro')
group by eco.comercio_recod
order by 2 desc), 
c as (select b.NOM_Comercio,b.N_TRX,SUM(SUM(b.N_TRX)) OVER (ORDER BY b.ID ASC) AS Acumulado,
        (SUM(SUM(b.N_TRX)) OVER (ORDER BY b.ID ASC))/SUM(SUM(b.N_TRX)) OVER (ORDER BY 1 ASC) Porc_Acum from a b
        group by b.NOM_Comercio,b.N_TRX,b.id),
Tabla_Final as (select b.user as identity,max(b.comercio_recod) as Mejor_Comercio
                from c a join `${project_source_1}.economics.economics` b
                on a.NOM_Comercio = b.comercio_recod
                        where Porc_Acum <= 0.8
                        and fecha >= date_add(CURRENT_DATE(),INTERVAL -3 MONTH)
                group by b.user)
SELECT
*
from Tabla_Final
);
