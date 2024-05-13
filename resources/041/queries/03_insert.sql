-->>>>> Creación tabla events <<<<<--

CREATE TABLE IF NOT EXISTS `${project_target}.churn.tablon_{{period}}_eventos_churn_bolsillo` 
PARTITION BY Fecha_Fin_Analisis_DT
CLUSTER BY user
AS (
    SELECT * 
    FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM `${project_target}.churn.tablon_{{period}}_eventos_churn_bolsillo` 
WHERE Fecha_Fin_Analisis_DT = (SELECT Fecha_Fin_Analisis_DT FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_0001_Params`);

INSERT INTO `${project_target}.churn.tablon_{{period}}_eventos_churn_bolsillo` 
SELECT * FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux`;

