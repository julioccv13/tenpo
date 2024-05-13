today=$(date +"%Y%m%d")
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.analytics_185654164.events_intraday_'$today'`' \
'AS
SELECT
 *
FROM
  `mine-app-001.analytics_185654164.events_intraday_'$today'`'


declare -a arr=($(seq 9))
for table in "${arr[@]}"
do
echo $table
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.analytics_185654164.events_2022120'$table'`' \
'AS
SELECT
 *
FROM
  `mine-app-001.analytics_185654164.events_2022110'$table'`'
done


declare -a arr=($(seq 10 30))
for table in "${arr[@]}"
do
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.analytics_185654164.events_202212'$table'`' \
'AS
SELECT
 *
FROM
  `mine-app-001.analytics_185654164.events_202211'$table'`'
done


declare -a arr=("ref_transaccion" "ref_recarga" "ref_producto" "ref_operador" "ref_comisiones" "ref_tipo_producto" "ref_estado" "ref_origen" "ref_medio_pago" "slp_puntos_rf" "slp_usuario" "bof_persona")
for table in "${arr[@]}"
do
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.recargas_2020.'$table'`' \
'AS
SELECT
 *
FROM
  `tenpo-rf.recargas_2020.'$table'`'
done


declare -a arr=("tablero_ejecutivo" "csat_ejecutivo" "csat" "cantidad_tickets_abiertos_daily" "fcr" "fcr_ejecutivo" "tmo" "llamadas_atendida_ejecutivo")
for table in "${arr[@]}"
do
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.control_tower.'$table'`' \
'AS
SELECT
 *
FROM
  `tenpo-sac-mart-prod.control_tower.'$table'`'
done


declare -a arr=("pay_gran_empresa" "pay_cliente" "pay_cuenta" "pay_transaccion" "pay_gran_empresa" "entidades" "pay_dolar" "data_consolidation")
for table in "${arr[@]}"
do
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.paypal_2020.'$table'`' \
'AS
SELECT
 *
FROM
  `hallowed-port-272015.paypal_2020.'$table'`'
done


declare -a arr=("ccam_movements" "ccam_pending_movements")
for table in "${arr[@]}"
do
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `tenpo-datalake-sandbox.tyba_tenpo.'$table'`' \
'AS
SELECT
 *
FROM
  `tyba-tenpo.tyba_tenpo.'$table'`'
done
