-->>>>> Creación tabla events <<<<<--

CREATE TABLE IF NOT EXISTS `{{project_id}}.churn.tablon_{{period}}_eventos_churn` 
PARTITION BY Fecha_Fin_Analisis_DT
CLUSTER BY user
AS (
    SELECT * 
    FROM `{{project_id}}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM `{{project_id}}.churn.tablon_{{period}}_eventos_churn` 
WHERE Fecha_Fin_Analisis_DT = (SELECT Fecha_Fin_Analisis_DT FROM `{{project_id}}.temp.CHURN_{{period}}_{{ds_nodash}}_0001_Params`);

INSERT INTO `{{project_id}}.churn.tablon_{{period}}_eventos_churn` 
SELECT * FROM `{{project_id}}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux`;

