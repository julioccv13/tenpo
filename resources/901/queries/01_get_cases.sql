DROP TABLE IF EXISTS `${project_target}.temp.p_dof_sent_max_date_{{ts_nodash}}`;
CREATE TABLE `${project_target}.temp.p_dof_sent_max_date_{{ts_nodash}}` AS (
    SELECT 
        user
        ,MAX(fecha_hora_creacion) as max_fecha_hora_creacion
    FROM `${project_target}.process_state.compliance_dof_sent`
    GROUP BY 1
);

DROP TABLE IF EXISTS `${project_target}.temp.p_dof_{{ts_nodash}}`;
CREATE TABLE `${project_target}.temp.p_dof_{{ts_nodash}}` AS (
  -- personal - frelance
    (SELECT distinct
        a.user
        ,c.rut
        ,c.first_name as nombres
        ,c.last_name as apellidos
        ,a.id_prp_movimiento
        ,a.trx_timestamp
        ,DATE(a.trx_timestamp  , "America/Santiago") AS fecha
        ,DATETIME(a.trx_timestamp  , "America/Santiago") AS fecha_hora_creacion
        ,a.monto_dolares
        ,a.monto as monto_pesos
        ,a.tipo
        ,c.email as email_persona
        ,c.email as email_empresa
        ,a.user as tenpo_user_id
        ,current_date() as execution_date
    FROM `${project_source_2}.compliance.dof` a
    LEFT JOIN `${project_target}.process_state.compliance_dof_sent` b
    ON a.id_prp_movimiento = b.id_prp_movimiento
    LEFT JOIN `${project_source_1}.users.users` c
    ON a.user = c.id
    LEFT JOIN `${project_target}.temp.p_dof_sent_max_date_{{ts_nodash}}` d
    ON a.user = d.user
    Where a.tipo in ('FREELANCE','PERSONAL')
    AND b.id_prp_movimiento IS NULL
    -- Regla 1 Mes Periodo de Gracia 
    AND ((d.max_fecha_hora_creacion IS NULL) OR date_diff(date(fecha_hora_creacion), date(d.max_fecha_hora_creacion), month) > 1) 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_prp_movimiento ORDER BY fecha_hora_creacion DESC) = 1)

----------------
    UNION ALL
----------------
-- business
    (SELECT distinct
        a.user
       ,f.rut as rut_empresa
       ,e.first_name as nombres
       ,e.last_name as apellidos
        ,a.id_prp_movimiento
        ,a.trx_timestamp
        ,DATE(a.trx_timestamp  , "America/Santiago") AS fecha
        ,DATETIME(a.trx_timestamp  , "America/Santiago") AS fecha_hora_creacion
        ,a.monto_dolares
        ,a.monto as monto_pesos
        ,a.tipo
        ,e.email as email_persona
        ,f.email as email_empresa
        ,f.tenpo_user_id
        ,current_date() as execution_date
        
    FROM `${project_source_2}.compliance.dof` a
    LEFT JOIN `${project_target}.process_state.compliance_dof_sent` b
    ON a.id_prp_movimiento = b.id_prp_movimiento
    LEFT JOIN `${project_source_2}.tenpo_business.transaction_history_business` d
    ON a.user = d.user_id
    LEFT JOIN `${project_source_1}.users.users` e
    ON d.tenpo_user_id = e.id    
    LEFT JOIN `${project_source_1}.business.business` f
    ON d.tenpo_user_id = f.tenpo_user_id
    LEFT JOIN `${project_target}.temp.p_dof_sent_max_date_{{ts_nodash}}` g
    ON a.user = g.user
    Where a.tipo ='BUSINESS'
    AND b.id_prp_movimiento IS NULL
    -- Regla 1 Mes Periodo de Gracia 
    AND ((g.max_fecha_hora_creacion IS NULL) OR date_diff(date(fecha_hora_creacion), date(g.max_fecha_hora_creacion), month) > 1)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_prp_movimiento ORDER BY fecha_hora_creacion DESC) = 1)
  
);
