DECLARE valor_uf FLOAT64 DEFAULT 0.0;

SET (valor_uf) = (SELECT AS STRUCT rate  
  FROM `tenpo-bi-prod.exchange_rate.unit_rate`
  WHERE verified_date = (SELECT MAX(verified_date) FROM `tenpo-bi-prod.exchange_rate.unit_rate`)
  )
;

DROP TABLE IF EXISTS `tenpo-bi.tmp.preventivo_cobro_seguro_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.preventivo_cobro_seguro_{{ds_nodash}}` AS 

(
-- reivsar caso en que una persona tiene mas de un sguro, en ese caso hacer group by para que la info que traiga la property, sera realaiconado al seguro que vene más proximamente
WITH TABLA_FINAL AS(
SELECT A.user_id AS identity, 
  A.nombre_seguro_a_pagar AS nombre_seguro_a_pagar,
  B.estado_balance, 
  DATE_DIFF(B.fecha_proximo_pago,CURRENT_DATE(),DAY) AS dias_por_pagar_seguro,
  B.fecha_proximo_pago AS fecha_proximo_pago_seguro,
  C.saldo_dia, 
  CASE
    WHEN B.ab_accumulated_debt_2 > 0 THEN B.ab_accumulated_debt_2 * valor_uf 
    WHEN B.ab_accumulated_debt_2 = 0 THEN B.p_net_2 * valor_uf
    END AS monto_poliza_seguro,
  CASE
    WHEN B.ab_accumulated_debt_2 > 0 AND (B.ab_accumulated_debt_2 * valor_uf > C.saldo_dia) THEN 1
    WHEN B.ab_accumulated_debt_2 = 0 AND (B.p_net_2 * valor_uf > C.saldo_dia) THEN 1
    ELSE 0
    END AS saldo_no_cubre_seguro
FROM (SELECT *
  FROM (
    SELECT *,
    CASE
      WHEN product_name LIKE '%Fallecimiento%' THEN 'Seguro de Vida con Telemedicina'
      WHEN product_name LIKE '%Auto%' THEN 'Seguro Auto Contenido Plus'
      ElSE 'Seguro'
      END AS nombre_seguro_a_pagar,
      row_number() over(partition by user_id order by creator_transaction_id DESC) as rn 
    FROM `tenpo-bi-prod.insurance.master_insurance`
    WHERE status = 'ACTIVE'
    AND payment_method_recod = 'Tarjeta Prepago'
    )
  WHERE rn = 1
  ) AS A
LEFT JOIN (SELECT creator_transaction_id, 
    CAST(ab_accumulated_debt AS FLOAT64) AS ab_accumulated_debt_2, 
    CAST(p_net AS FLOAT64) AS p_net_2,
    ab_balance_statement AS estado_balance,
    DATE(ab_next_billing_date) AS fecha_proximo_pago
  FROM `tenpo-bi-prod.insurance.policy`
  WHERE status = "ACTIVE" AND payment_method = "PREPAID"
  ORDER BY creator_transaction_id
  ) AS B ON A.creator_transaction_id = B.creator_transaction_id
LEFT JOIN (SELECT user AS identity, CAST(saldo_dia AS int) saldo_dia
  FROM `tenpo-bi-prod.balance.daily_balance`
  where fecha = (SELECT MAX(fecha) FROM `tenpo-bi-prod.balance.daily_balance`)
  and segmento = 'PERSONAL'
  ) AS C ON A.user_id = C.identity
WHERE B.fecha_proximo_pago IS NOT NULL
)
SELECT distinct A.id AS identity, 
IFNULL(CAST(B.nombre_seguro_a_pagar AS string),'N/A') AS nombre_seguro_a_pagar, 
IFNULL(CAST(B.dias_por_pagar_seguro AS string),'N/A') AS dias_por_pagar_seguro, 
IFNULL(CAST(B.fecha_proximo_pago_seguro AS string),'N/A') AS fecha_proximo_pago_seguro, 
IFNULL(CAST(B.saldo_no_cubre_seguro AS string),'N/A') AS saldo_no_cubre_seguro
FROM `tenpo-bi-prod.users.users_tenpo`  AS A
LEFT JOIN TABLA_FINAL AS B ON A.id = B.identity
WHERE A.status_onboarding = 'completo' AND A.state = 4

)