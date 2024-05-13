CREATE TABLE IF NOT EXISTS `${project_target}.external.call_south`
AS (
    SELECT 
        *
    FROM `${project_source}.temp.call_south_sftp_{{ds_nodash}}`
    LIMIT 0
);


DELETE FROM `${project_target}.external.call_south`
WHERE 
    execution_month = (SELECT DISTINCT execution_month FROM `${project_source}.temp.call_south_sftp_{{ds_nodash}}`);

INSERT INTO `${project_target}.external.call_south`
SELECT 
    *
FROM `${project_source}.temp.call_south_sftp_{{ds_nodash}}`;
