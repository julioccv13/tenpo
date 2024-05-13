DROP TABLE IF EXISTS `${project_target}.crm.PASO_ALEATORIO`;

CREATE TABLE `${project_target}.crm.PASO_ALEATORIO` AS (
    
    SELECT 
        A.ID, 
        CASE WHEN RAND() <= 0.5 THEN 0 ELSE 1 END AS RAND
    FROM `${project_source}.users.users_tenpo` A
    LEFT JOIN `${project_target}.crm.CLIENTES_ALEATORIO` B on A.ID = B.ID
    WHERE B.id IS NULL
    AND STATE = 4


);

INSERT INTO `${project_target}.crm.CLIENTES_ALEATORIO`
SELECT * FROM `${project_target}.crm.PASO_ALEATORIO`