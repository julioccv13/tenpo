CREATE TABLE IF NOT EXISTS `${project_target}.ingestas_api.cmf_api_dolar` AS (
    SELECT *
    FROM `${project_target}.temp.DE001_{{ds_nodash}}_Dolares`
    LIMIT 0
);

DELETE FROM `${project_target}.ingestas_api.cmf_api_dolar`
WHERE fecha in (SELECT DISTINCT fecha FROM `${project_target}.temp.DE001_{{ds_nodash}}_Dolares`);

INSERT INTO `${project_target}.ingestas_api.cmf_api_dolar`
SELECT * FROM `${project_target}.temp.DE001_{{ds_nodash}}_Dolares`;