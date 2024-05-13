--DROP TABLE IF EXISTS `tenpo-datalake-prod.integrations.selyt_leads`;
CREATE TABLE IF NOT EXISTS `${project_target}.integrations.selyt_leads`
(
    idoportunidad STRING
    ,fecha_oportunidad STRING
    ,rut_cliente STRING
    ,nombre_cliente STRING
    ,mail_cliente STRING
    ,telefono_cliente INT64
);

DELETE FROM `${project_target}.integrations.selyt_leads` 
WHERE rut_cliente in (SELECT DISTINCT rut_cliente from `${project_target}.temp.selyt_out_${ds_nodash}`);

INSERT INTO `${project_target}.integrations.selyt_leads` 
SELECT 
    idoportunidad
    ,fecha_oportunidad
    ,rut_cliente
    ,nombre_cliente
    ,mail_cliente 
    ,telefono_cliente   
FROM `${project_target}.temp.selyt_out_${ds_nodash}`;