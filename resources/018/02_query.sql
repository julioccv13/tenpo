/*

  project_id: tenpo-bi
  project_source_1: tenpo-airflow-prod

*/

DROP TABLE IF EXISTS `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Activity_Aux`;
CREATE TABLE  `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Activity_Aux` AS (

WITH
  last_fee_vl as (
    SELECT 
        amount last_fee_value
    FROM `{{project_source_1}}.payment_savings.fees` 
    LEFT JOIN `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params` ON 1=1 
    WHERE 
        CAST(valid_on_date AS DATE ) <= Fecha_Analisis  
    ORDER BY 
        valid_on_date DESC 
    LIMIT 1
    ),
  trx_reglas as (
    select distinct cash_in_id from `{{project_source_1}}.savings_rules.rule_execution_history`
  ),
  cashins as (
    SELECT DISTINCT
      cash_in_id trx_id,
      user_id user,
      pending_date,
      fee_quantity,
      last_fee_value ,
      fee_quantity * last_fee_value  amount,
      'aporte' tipo,
      trx_reglas.cash_in_id is not null as is_rule
    FROM `{{project_source_1}}.payment_savings.cash_in` ci
    LEFT JOIN trx_reglas using (cash_in_id) 
    LEFT JOIN last_fee_vl ON 1=1 
    LEFT JOIN `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params` ON 1=1
    WHERE 
      status = 'SUCCESSFUL' 
      AND  CAST(created_date AS DATE) <= Fecha_Analisis  
      ),
  
  cashouts as (
    SELECT DISTINCT
      cash_out_id trx_id,
      user_id user,
      pending_date,
      fee_quantity,
      last_fee_value ,
      fee_quantity *last_fee_value*-1 amount,
      'rescate' tipo,
      false as is_rule
    FROM `{{project_source_1}}.payment_savings.cash_out` 
    LEFT JOIN last_fee_vl ON 1=1
    LEFT JOIN `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params` ON 1=1
    WHERE 
      status in ( 'SUCCESSFUL', 'PENDING_SETTLEMENT')
      AND  CAST(created_date AS DATE) <= Fecha_Analisis 
    ),
    
  cashins_no_invertidos as (
     SELECT DISTINCT
      cash_in_id trx_id,
      user_id user,
      pending_date,
      amount,
      'aporte no invertido' tipo,
      trx_reglas.cash_in_id is not null as is_rule
    FROM `{{project_source_1}}.payment_savings.cash_in` ci
    LEFT JOIN trx_reglas using (cash_in_id) 
    LEFT JOIN last_fee_vl ON 1=1
    LEFT JOIN `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params` ON 1=1
    WHERE 
      status in ( 'CREATED', 'PENDING', 'CONFIRMED', 'REJECTED')
      AND  CAST(created_date AS DATE) <= Fecha_Analisis 
       ),
   
  cashouts_no_invertidos as (
    SELECT DISTINCT
      cash_out_id trx_id,
      user_id user,
      pending_date,
      amount*-1 amount,
      'rescate no invertido' tipo,
      false as is_rule
    FROM `{{project_source_1}}.payment_savings.cash_out` 
    LEFT JOIN last_fee_vl ON 1=1
    LEFT JOIN `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params` ON 1=1
    WHERE 
      status in ( 'CREATED', 'PENDING', 'CONFIRMED', 'REJECTED') 
      AND  CAST(created_date AS DATE) <= Fecha_Analisis 
      )
    
    
    SELECT
      Fecha_Analisis,
      user,
      trx_id,
      amount,
      tipo,
      is_rule
    FROM(
      SELECT 
        user,
        trx_id,
        amount,
        tipo,
        is_rule
      FROM cashins UNION ALL
      SELECT 
        user,
        trx_id,
        amount,
        tipo,
        is_rule
      FROM cashouts UNION ALL
      SELECT
        user,
        trx_id,
        amount,
        tipo,
        is_rule
      FROM cashins_no_invertidos UNION ALL
      SELECT
        user,
        trx_id,
        amount,
        tipo,
        is_rule
      FROM cashouts_no_invertidos
    )
    LEFT JOIN  `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params`  ON 1=1

);
