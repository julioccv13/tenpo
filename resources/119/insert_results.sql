-- INSERT IDEMPOTENTE
CREATE TABLE IF NOT EXISTS `${project_target}.jarvis.event_string_query` AS (
    SELECT * 
    FROM `${project_target}.jarvis.event_string_query_{{ds_nodash}}`
    LIMIT 0
);

-- DELETE DATA BEFORE INSERT
TRUNCATE TABLE `${project_target}.jarvis.event_string_query`;

INSERT INTO `${project_target}.jarvis.event_string_query`
SELECT * FROM `${project_target}.jarvis.event_string_query_{{ds_nodash}}`;

-- LOAD SNAPSHOT FROM AN HOUR AGO USING TIME TRAVEL
DROP TABLE IF EXISTS `${project_target}.jarvis.event_string_query_snapshot`;
CREATE TABLE  `${project_target}.jarvis.event_string_query_snapshot` AS 

( SELECT *
  FROM  `${project_target}.jarvis.event_string_query`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)
);



-- COMPARE TWO TABLES AND CREATE A TABLE THAT CONTAINS USERS WHO CHANGE ONE OF THEIR ATTRIBUTES
DROP TABLE IF EXISTS `${project_target}.jarvis.target_event_string_query`;
CREATE TABLE `${project_target}.jarvis.target_event_string_query` AS 
(
  (
    SELECT * FROM `${project_target}.jarvis.event_string_query`
    EXCEPT DISTINCT
    SELECT * FROM `${project_target}.jarvis.event_string_query_snapshot`   
  )

UNION ALL

  (
    SELECT * FROM `${project_target}.jarvis.event_string_query_snapshot`
    EXCEPT DISTINCT
    SELECT * FROM `${project_target}.jarvis.event_string_query`
  )
)
;
