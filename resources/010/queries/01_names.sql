DROP TABLE IF EXISTS `${project_target}.temp.P_{{ds_nodash}}_Names_Aux`;
CREATE TABLE `${project_target}.temp.P_{{ds_nodash}}_Names_Aux` AS (
    SELECT 
    id
    ,REGEXP_REPLACE(NORMALIZE(LOWER(ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(SPLIT(first_name, ' ')) AS x WITH OFFSET off WHERE off < 1 ORDER BY off), ' '))), r"\pM", '') as first_name
    ,ob_completed_at as fecha
    ,'tenpo' as origin
    FROM `${project_source_1}.users.users`
);

INSERT INTO `${project_target}.temp.P_{{ds_nodash}}_Names_Aux`
SELECT 
CAST(id_persona AS STRING) AS id
,REGEXP_REPLACE(NORMALIZE(LOWER(ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(SPLIT(nombre, ' ')) AS x WITH OFFSET off WHERE off < 1 ORDER BY off), ' '))), r"\pM", '') as first_name
,fecha_creacion as fecha
,'recargas_web' as origin
FROM `${project_source_2}.recargas_2020.bof_persona`;


INSERT INTO `${project_target}.temp.P_{{ds_nodash}}_Names_Aux`
SELECT 
CAST(rut AS STRING) AS id
,REGEXP_REPLACE(NORMALIZE(LOWER(ARRAY_TO_STRING(ARRAY(SELECT x FROM UNNEST(SPLIT(nombre, ' ')) AS x WITH OFFSET off WHERE off < 1 ORDER BY off), ' '))), r"\pM", '') as first_name
,fecha_ingreso_registro as fecha
,'paypal_web' as origin
FROM `${project_source_3}.paypal_2020.entidades`;


INSERT INTO `${project_target}.aux_table.name_genders` 
with all_names as (
    SELECT 
        first_name, name, gender, probability, count
    FROM `${project_target}.aux_table.name_genders`
    WHERE true
    QUALIFY ROW_NUMBER() OVER (PARTITION BY first_name ORDER BY count DESC) = 1
), non_existing_ids as (
    SELECT 
        a.*
    FROM `${project_target}.temp.P_{{ds_nodash}}_Names_Aux` a
    LEFT JOIN `${project_target}.aux_table.name_genders` b
    ON a.id = b.id and a.origin = b.origin
    WHERE b.id IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY fecha DESC) = 1
)
SELECT 
    a.id, a.first_name, a.fecha, a.origin,
    b.name, b.gender, b.probability, b.count
FROM non_existing_ids a
LEFT JOIN all_names b
ON a.first_name = b.first_name
WHERE b.first_name IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY fecha DESC) = 1;


DROP TABLE IF EXISTS `${project_target}.temp.P_{{ds_nodash}}_Names`;
CREATE TABLE `${project_target}.temp.P_{{ds_nodash}}_Names` AS (
    SELECT 
        DISTINCT a.first_name
    FROM `${project_target}.temp.P_{{ds_nodash}}_Names_Aux` a
    LEFT JOIN `${project_target}.aux_table.name_genders` b
    ON a.first_name = b.first_name
    WHERE b.first_name IS NULL
);