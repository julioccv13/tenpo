DROP TABLE IF EXISTS `tenpo-bi.tmp.reco_react_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.reco_react_{{ds_nodash}}` AS 
(
  
WITH ultima_fecha AS (
    SELECT 
        user, 
        MAX(fecha_ejecucion) as max_fecha
    FROM `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user`
    GROUP BY user
)
SELECT uf.user as identity, 
    cr.prod_1_reactivacion AS reactivacion_1, 
    cr.prod_2_reactivacion AS reactivacion_2, 
    cr.prod_3_reactivacion AS reactivacion_3, 
    cr.recomendacion_1, 
    cr.recomendacion_2, 
    cr.recomendacion_3,
FROM ultima_fecha uf
JOIN `tenpo-bi-prod.modelos_ds.consolidado_recomendacion_user` cr ON uf.user = cr.user AND uf.max_fecha = cr.fecha_ejecucion

)
