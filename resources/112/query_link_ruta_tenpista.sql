DROP TABLE IF EXISTS `tenpo-bi.tmp.links_ruta_tenpista_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.links_ruta_tenpista_{{ds_nodash}}` AS 
(
SELECT A.id AS identity,
CASE
  WHEN B.sigla_region = 'METROPOLITANA DE SANTIAGO' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/01.png" alt="Santiago" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN B.sigla_region = 'TARAPACA' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/02.png" alt="Iquique" style="max-width: 100%; display:block; margin: 0 auto;">' 
  WHEN B.sigla_region = 'ANTOFAGASTA' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/03.png" alt="Antofagasta" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN B.sigla_region = 'VALPARAISO' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/04.png" alt="Valparaíso y Viña del Mar" style="max-width: 100%; display:block; margin: 0 auto;">' 
  WHEN B.sigla_region = 'MAULE' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/05.png" alt="Talca y Curicó" style="max-width: 100%; display:block; margin: 0 auto;">' 
  WHEN B.sigla_region = 'BIOBIO' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/06.png" alt="Concepción Talcahuano" style="max-width: 100%; display:block; margin: 0 auto;">' 
  WHEN B.sigla_region = 'LA ARAUCANIA' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/07.png" alt="Temuco" style="max-width: 100%; display:block; margin: 0 auto;">' 
  WHEN B.sigla_region = 'LOS LAGOS' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/08.png" alt="Puerto Varas" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN B.sigla_region = 'COQUIMBO' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/ruta/09.png" alt="Coquimbo" style="max-width: 100%; display:block; margin: 0 auto;">'
  ELSE 'N/A'
END AS link_ruta_tenpista
FROM (SELECT *
  FROM `tenpo-bi-prod.users.users_tenpo` as A
  WHERE A.status_onboarding = 'completo' and A.state = 4
  ) AS A
LEFT JOIN (SELECT *
  FROM `tenpo-bi-prod.users.demographics`) AS B ON A.id = B.id_usuario
)