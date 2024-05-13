DROP TABLE IF EXISTS `${project_source_2}.temp.mttr_cantidad_tickets_abiertos_daily_{{ds_nodash}}`;
CREATE TABLE `${project_source_2}.temp.mttr_cantidad_tickets_abiertos_daily_{{ds_nodash}}` AS (
  with table1 as (
      SELECT CAST(PARSE_DATE("%Y%m%d", "{{ds_nodash}}") AS timestamp) AS Fecha_Ejec
      ), 
  table2 as (    
      select creacion, grupo, origen, (select Fecha_Ejec from table1) as fecha_consulta, TIMESTAMP_DIFF((select Fecha_Ejec from table1),creacion, minute)/(60*24) as day_diff from `${project_source_1}.external.tickets_freshdesk`
      where creacion>='2022-01-01'
      and creacion <= (select Fecha_Ejec from table1)
      and timestamp_mttr >= (select Fecha_Ejec from table1)
  )
  
  select fecha_consulta as hora_inicial, grupo, origen, avg(day_diff) as avg_mttr_day from table2
  group by 1,2,3
);

DELETE FROM `${project_target}.control_tower.mttr_cantidad_tickets_abiertos_daily`
WHERE 
    hora_inicial = (SELECT distinct hora_inicial from `${project_source_2}.temp.mttr_cantidad_tickets_abiertos_daily_{{ds_nodash}}`);


INSERT INTO `${project_target}.control_tower.mttr_cantidad_tickets_abiertos_daily`
SELECT grupo, origen, avg_mttr_day, hora_inicial FROM `${project_source_2}.temp.mttr_cantidad_tickets_abiertos_daily_{{ds_nodash}}`;


