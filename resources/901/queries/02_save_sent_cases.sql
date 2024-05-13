CREATE TABLE IF NOT EXISTS `${project_target}.process_state.compliance_dof_sent` as (
    SELECT 
        *
    FROM `${project_target}.temp.p_dof_{{ts_nodash}}`
    LIMIT 0
);

INSERT INTO `${project_target}.process_state.compliance_dof_sent`
SELECT * FROM `${project_target}.temp.p_dof_{{ts_nodash}}`
WHERE 
    id_prp_movimiento NOT IN (
        SELECT distinct id_prp_movimiento FROM `${project_target}.process_state.compliance_dof_sent`
);