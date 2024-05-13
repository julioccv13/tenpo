DROP TABLE IF EXISTS `${project_target}.temp.P_{{ds_nodash}}_104_Temp`;
CREATE TABLE `${project_target}.temp.P_{{ds_nodash}}_104_Temp` AS (
        with datos as (
        SELECT
            *,
            CASE
              WHEN recency > 600 THEN 1
              WHEN recency > 400 THEN 2
              WHEN recency > 200 THEN 3
              WHEN recency <= 200 THEN 4
              END AS rfmp_recency
            ,CASE
               WHEN cuenta_trx_origen > 10 THEN 4
               WHEN cuenta_trx_origen > 5 THEN 3
               WHEN cuenta_trx_origen > 1 THEN 2
               WHEN  cuenta_trx_origen <= 1 THEN 1
               ELSE null
               END AS rfmp_frequency
            , CASE
               WHEN monto_gastado_origen > 100 THEN 4
               WHEN monto_gastado_origen > 60 THEN 3
               WHEN monto_gastado_origen > 20  THEN 2
               WHEN monto_gastado_origen <= 20 AND monto_gastado_origen >= 0 THEN 1
               ELSE null
               END AS rfmp_amount
            , CASE  
               WHEN uniq_productos_origen = 1 THEN 2
               WHEN uniq_productos_origen = 2 THEN 3
               WHEN uniq_productos_origen = 3 THEN 4
               WHEN uniq_productos_origen >=4 THEN 4
               ELSE null
               END as rfmp_product
            ,CASE
              WHEN monto_gastado_ultm360d is null THEN null
              WHEN recency_ult360d > 120 THEN 1
              WHEN recency_ult360d > 90 THEN 2
              WHEN recency_ult360d > 60 THEN 3
              WHEN recency_ult360d <= 60 THEN 4
              END AS rfmp_recency_ult360d
            ,CASE
               WHEN monto_gastado_ultm360d is null THEN null
               WHEN monto_gastado_ultm360d > 120 THEN 4
               WHEN monto_gastado_ultm360d > 60 THEN 3
               WHEN monto_gastado_ultm360d > 20 THEN 2
               WHEN  monto_gastado_ultm360d <= 20 THEN 1
               ELSE null
               END AS rfmp_amount_ult360d
            ,CASE
               WHEN monto_gastado_ultm360d is null THEN null
               WHEN cuenta_trx_ultm_360d > 10 THEN 4
               WHEN cuenta_trx_ultm_360d > 5 THEN 3
               WHEN cuenta_trx_ultm_360d > 3 THEN 2
               WHEN cuenta_trx_ultm_360d <= 3 THEN 1
               ELSE null
               END AS rfmp_frequency_ult360d
            ,CASE 
               WHEN monto_gastado_ultm360d is null THEN null
               WHEN uniq_productos_ult360dias = 1  THEN 1
               WHEN uniq_productos_ult360dias in (2) THEN 3
               WHEN uniq_productos_ult360dias in (3) THEN 4
               WHEN uniq_productos_ult360dias >= 4 THEN 4
               ELSE null
               END as rfmp_product_ult360d
        FROM `${project_target}.temp.P_{{ds_nodash}}_104_Temp_Data`
    ),
    calculo_score as (
        SELECT
          a.*
          ,rfmp_product + rfmp_amount + rfmp_frequency + rfmp_recency score
          ,rfmp_product_ult360d + rfmp_amount_ult360d + rfmp_frequency_ult360d + rfmp_recency score_ult60d
        FROM datos a
    ),
    calculo_segmento as (
        SELECT DISTINCT
          a.*
          ,CASE 
            WHEN score > 13 AND score <= 16 THEN 'gold'
            WHEN score > 10 AND score <= 13 THEN 'silver'
            WHEN score > 7 AND score <= 10 THEN 'bronze'
            WHEN  score <= 7 AND score >= 4 THEN 'green'
            ELSE null
            END as segment
          ,CASE 
            WHEN score_ult60d > 13 AND score_ult60d <= 16 THEN 'gold'
            WHEN score_ult60d > 10 AND score_ult60d <= 13 THEN 'silver'
            WHEN score_ult60d > 7 AND score_ult60d <= 10 THEN 'bronze'
            WHEN score_ult60d <= 7 AND score_ult60d >=4 THEN 'green'
            ELSE null
            END as segment_ult60d
        FROM calculo_score a )
    SELECT 
        * EXCEPT (segment_ult60d),
        IF(segment_ult60d is null AND segment is not null, 'dormido' , segment_ult60d) segment_ult60d
    FROM calculo_segmento
);