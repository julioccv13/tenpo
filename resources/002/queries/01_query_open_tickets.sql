DROP TABLE IF EXISTS `${project_source_2}.temp.cantidad_tickets_abiertos_daily_{{ds_nodash}}`;
CREATE TABLE `${project_source_2}.temp.cantidad_tickets_abiertos_daily_{{ds_nodash}}` AS (
with table1 as (
    SELECT CAST(PARSE_DATE("%Y%m%d", "{{ds_nodash}}") AS timestamp) AS Fecha_Ejec
    )
    
    select origen, grupo, count(*) as cantidad_tickets_abiertos, (select Fecha_Ejec from table1) as hora_inicial from `${project_source_1}.external.tickets_freshdesk`
    where creacion>='2022-01-01'
    and creacion <= (select Fecha_Ejec from table1)
    and timestamp_mttr >= (select Fecha_Ejec from table1)
    group by 1,2
)
;

CREATE TABLE IF NOT EXISTS `${project_target}.control_tower.cantidad_tickets_abiertos_daily` 
(origen STRING, grupo STRING,cantidad_tickets_abiertos INT64, hora_inicial TIMESTAMP)
;


DELETE FROM `${project_target}.control_tower.cantidad_tickets_abiertos_daily`
WHERE 
    hora_inicial = (SELECT distinct hora_inicial from `${project_source_2}.temp.cantidad_tickets_abiertos_daily_{{ds_nodash}}`);


INSERT INTO `${project_target}.control_tower.cantidad_tickets_abiertos_daily`
SELECT origen, grupo, cantidad_tickets_abiertos, hora_inicial FROM `${project_source_2}.temp.cantidad_tickets_abiertos_daily_{{ds_nodash}}`;

