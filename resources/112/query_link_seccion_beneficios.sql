
DROP TABLE IF EXISTS `tenpo-bi.tmp.links_seccion_beneficios_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.links_seccion_beneficios_{{ds_nodash}}` AS 
(


SELECT A.identity,
CASE 
  WHEN seccion_beneficio1 = 'Foodie' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/02.png" alt="Foodie" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio1 = 'Ecommerce' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/03.png" alt="Ecommerce" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio1 = 'Entretenimiento' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/04.png" alt="Entretenimiento" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio1 = 'Transporte' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/06.png" alt="Automóvil" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio1 = 'Estudios y trabajo' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/07.png" alt="Estudios y Trabajo" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio1 = 'Mascotas' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/08.png" alt="Mascotas" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio1 = 'Salud' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/09.png" alt="Salud" style="max-width: 100%; display:block; margin: 0 auto;">'
  ELSE 'N/A'
END AS link_seccion_beneficios1,
CASE 
  WHEN seccion_beneficio2 = 'Foodie' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/02.png" alt="Foodie" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio2 = 'Ecommerce' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/03.png" alt="Ecommerce" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio2 = 'Entretenimiento' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/04.png" alt="Entretenimiento" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio2 = 'Transporte' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/06.png" alt="Automóvil" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio2 = 'Estudios y trabajo' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/07.png" alt="Estudios y Trabajo" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio2 = 'Mascotas' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/08.png" alt="Mascotas" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio2 = 'Salud' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/09.png" alt="Salud" style="max-width: 100%; display:block; margin: 0 auto;">'
  ELSE 'N/A'
END AS link_seccion_beneficios2,

CASE 
  WHEN seccion_beneficio3 = 'Foodie' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/02.png" alt="Foodie" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio3 = 'Ecommerce' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/03.png" alt="Ecommerce" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio3 = 'Entretenimiento' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/04.png" alt="Entretenimiento" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio3 = 'Transporte' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/06.png" alt="Automóvil" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio3 = 'Estudios y trabajo' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/07.png" alt="Estudios y Trabajo" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio3 = 'Mascotas' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/08.png" alt="Mascotas" style="max-width: 100%; display:block; margin: 0 auto;">'
  WHEN seccion_beneficio3 = 'Salud' THEN '<img src="https://tenpoimagesstorage2.blob.core.windows.net/marketing/03_dco_2023/beneficios/09.png" alt="Salud" style="max-width: 100%; display:block; margin: 0 auto;">'
  ELSE 'N/A'
END AS link_seccion_beneficios3,
FROM (

   with rubro_1 as (
    SELECT 
      A.user,
      A.rubro,
      CASE
        WHEN A.rubro IN ('alimentos','cafeterias','delivery','expendedoras','restaurante','supermercado') THEN 'Foodie'
        WHEN A.rubro IN ('aliexpress','amazon','apple','articulos de oficina','calzado','construcción','gas','lavanderías','libros','licorerias','marketplace','música','retail','servicios basicos','tecnología','telecomunicaciones') THEN 'Ecommerce'
        WHEN A.rubro IN ('cine','deporte','apuestas','ocio','red social','streaming','videojuegos') THEN 'Entretenimiento'
        WHEN A.rubro IN ('transporte','combustible','automotriz','parking','pasajes bus','transporte','transporte publico','viajes','vuelos') THEN 'Transporte'
        WHEN A.rubro IN ('software','estudios','criptomonedas','cuentas') THEN 'Estudios y trabajo'
        WHEN A.rubro IN ('mascotas') THEN 'Mascotas'
        WHEN A.rubro IN ('seguros','salud') THEN 'Salud'
        ELSE 'Otro' END AS seccion_beneficio,
      A.corr,
      A.rn_up
    FROM 
    (
      SELECT A.user, A.rubro, A.corr, ROW_NUMBER() OVER (PARTITION BY A.user ORDER BY A.corr DESC) AS rn_up
      FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador` AS A
      WHERE fecha_ejecucion = (SELECT MAX(fecha_ejecucion) FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador`)
    ) AS A
    WHERE A.rn_up = 1 
    )
    ,
    rubro_2 as (
    SELECT 
      A.user,
      A.rubro,
      CASE
        WHEN A.rubro IN ('alimentos','cafeterias','delivery','expendedoras','restaurante','supermercado') THEN 'Foodie'
        WHEN A.rubro IN ('aliexpress','amazon','apple','articulos de oficina','calzado','construcción','gas','lavanderías','libros','licorerias','marketplace','música','retail','servicios basicos','tecnología','telecomunicaciones') THEN 'Ecommerce'
        WHEN A.rubro IN ('cine','deporte','apuestas','ocio','red social','streaming','videojuegos') THEN 'Entretenimiento'
        WHEN A.rubro IN ('transporte','combustible','automotriz','parking','pasajes bus','transporte','transporte publico','viajes','vuelos') THEN 'Transporte'
        WHEN A.rubro IN ('software','estudios','criptomonedas','cuentas') THEN 'Estudios y trabajo'
        WHEN A.rubro IN ('mascotas') THEN 'Mascotas'
        WHEN A.rubro IN ('seguros','salud') THEN 'Salud'
        ELSE 'Otro' END AS seccion_beneficio,
      A.corr,
      A.rn_up
    FROM 
    (
      SELECT A.user, A.rubro, A.corr, ROW_NUMBER() OVER (PARTITION BY A.user ORDER BY A.corr DESC) AS rn_up
      FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador` AS A
      WHERE fecha_ejecucion = (SELECT MAX(fecha_ejecucion) FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador`)
    ) AS A
    WHERE A.rn_up = 2 
    )
    ,
    rubro_3 as (
    SELECT 
      A.user,
      A.rubro,
      CASE
        WHEN A.rubro IN ('alimentos','cafeterias','delivery','expendedoras','restaurante','supermercado') THEN 'Foodie'
        WHEN A.rubro IN ('aliexpress','amazon','apple','articulos de oficina','calzado','construcción','gas','lavanderías','libros','licorerias','marketplace','música','retail','servicios basicos','tecnología','telecomunicaciones') THEN 'Ecommerce'
        WHEN A.rubro IN ('cine','deporte','apuestas','ocio','red social','streaming','videojuegos') THEN 'Entretenimiento'
        WHEN A.rubro IN ('transporte','combustible','automotriz','parking','pasajes bus','transporte','transporte publico','viajes','vuelos') THEN 'Transporte'
        WHEN A.rubro IN ('software','estudios','criptomonedas','cuentas') THEN 'Estudios y trabajo'
        WHEN A.rubro IN ('mascotas') THEN 'Mascotas'
        WHEN A.rubro IN ('seguros','salud') THEN 'Salud'
        ELSE 'Otro' END AS seccion_beneficio,
      A.corr,
      A.rn_up
    FROM 
    (
      SELECT A.user, A.rubro, A.corr, ROW_NUMBER() OVER (PARTITION BY A.user ORDER BY A.corr DESC) AS rn_up
      FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador` AS A
      WHERE fecha_ejecucion = (SELECT MAX(fecha_ejecucion) FROM `tenpo-datalake-prod.jarvis.output_prediccion_sistema_recomendador`)
    ) AS A
    WHERE A.rn_up = 3 
    )
    SELECT A.id AS identity, coalesce(B.seccion_beneficio,'N/A') AS seccion_beneficio1,
    CASE 
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Foodie' THEN 'Ecommerce'
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Ecommerce' THEN 'Foodie'
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Entretenimiento' THEN 'Ecommerce'
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Transporte' THEN 'Ecommerce'
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Estudios y trabajo' THEN 'Ecommerce'
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Mascotas' THEN 'Ecommerce'
      WHEN B.seccion_beneficio = C.seccion_beneficio AND B.seccion_beneficio = 'Salud' THEN 'Ecommerce'
      ELSE coalesce(C.seccion_beneficio,'N/A') END AS seccion_beneficio2,
    CASE
      WHEN (D.seccion_beneficio = B.seccion_beneficio OR D.seccion_beneficio = C.seccion_beneficio OR B.seccion_beneficio = C.seccion_beneficio) AND (D.seccion_beneficio NOT IN ('Entretenimiento') AND B.seccion_beneficio NOT IN ('Entretenimiento') AND C.seccion_beneficio NOT IN ('Entretenimiento')) THEN 'Entretenimiento'
      WHEN (D.seccion_beneficio = B.seccion_beneficio OR D.seccion_beneficio = C.seccion_beneficio OR B.seccion_beneficio = C.seccion_beneficio) AND (D.seccion_beneficio NOT IN ('Salud') AND B.seccion_beneficio NOT IN ('Salud') AND C.seccion_beneficio NOT IN ('Salud')) THEN 'Salud'
      WHEN (D.seccion_beneficio = B.seccion_beneficio OR D.seccion_beneficio = C.seccion_beneficio OR B.seccion_beneficio = C.seccion_beneficio) AND (D.seccion_beneficio NOT IN ('Transporte') AND B.seccion_beneficio NOT IN ('Transporte') AND C.seccion_beneficio NOT IN ('Transporte')) THEN 'Transporte'
      WHEN (D.seccion_beneficio = B.seccion_beneficio OR D.seccion_beneficio = C.seccion_beneficio OR B.seccion_beneficio = C.seccion_beneficio) AND (D.seccion_beneficio NOT IN ('Estudios y trabajo') AND B.seccion_beneficio NOT IN ('Estudios y trabajo') AND C.seccion_beneficio NOT IN ('Estudios y trabajo')) THEN 'Estudios y trabajo'
      WHEN (D.seccion_beneficio = B.seccion_beneficio OR D.seccion_beneficio = C.seccion_beneficio OR B.seccion_beneficio = C.seccion_beneficio) AND (D.seccion_beneficio IN ('Ecommerce') OR B.seccion_beneficio IN ('Ecommerce') OR C.seccion_beneficio IN ('Ecommerce')) THEN 'Salud'
      ELSE coalesce(D.seccion_beneficio,'N/A') END AS seccion_beneficio3
    FROM `tenpo-bi-prod.users.users_tenpo` as A
    LEFT JOIN rubro_1 AS B ON A.id = B.user
    LEFT JOIN rubro_2 AS C ON A.id = C.user
    LEFT JOIN rubro_3 AS D ON A.id = D.user
    WHERE A.status_onboarding = 'completo' and A.state = 4 --AND B.rubro IS NOT NULL
    
  
) AS A


)
