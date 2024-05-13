-- Creates just in case
CREATE TABLE IF NOT EXISTS `${project_target}.aux_table.clevertap_{{period}}_injection_new`
PARTITION BY date(fecha_ejec)
CLUSTER BY identity
AS (
    SELECT 
        *
    FROM `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM `${project_target}.aux_table.clevertap_{{period}}_injection_new` 

WHERE 
    fecha_ejec = (SELECT MAX(fecha_ejec) FROM `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}`);

INSERT INTO `${project_target}.aux_table.clevertap_{{period}}_injection_new` 
SELECT 
    *
FROM `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}`
;
