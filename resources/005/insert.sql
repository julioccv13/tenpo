CREATE TABLE IF NOT EXISTS `${project_target}.external.tickets_freshdesk_daily`
AS (
    SELECT 
        *
    FROM `${project_source}.temp.daily_tickets_{{ds_nodash}}`
    LIMIT 0
);


DELETE FROM `${project_target}.external.tickets_freshdesk_daily`
WHERE 
    execution_date = (SELECT DISTINCT execution_date FROM `${project_source}.temp.daily_tickets_{{ds_nodash}}`);

INSERT INTO `${project_target}.external.tickets_freshdesk_daily`
SELECT 
    *
FROM `${project_source}.temp.daily_tickets_{{ds_nodash}}`;
