DROP TABLE IF EXISTS `${project_target}.tmp.modelo_clv_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.modelo_clv_{{ds_nodash}}` AS (

SELECT user as identity, clv_mastercard,clv_mastercard_f,clv_paypal,clv_tyba,clv_utility_payments,(clv_mastercard+clv_mastercard_f+clv_paypal+clv_tyba+clv_utility_payments) as clv_total from `${project_source_1}.modelos_ds.clv`  where fecha_ejecucion = (select max(fecha_ejecucion) from `${project_source_1}.modelos_ds.clv` )
)
