DROP TABLE IF EXISTS `${project_target}.tmp.e1d`;
CREATE TABLE`${project_target}.tmp.e1d` AS (
with ruts as(
    SELECT distinct 
        rut,
        rand() as random
    FROM `${project_source_3}.e1d.registro_empresas_sociedades`
    where codigo_de_sociedad = 'EIRL'
        AND rut NOT IN (SELECT DISTINCT rut FROM `${project_source_3}.e1d.metadata_pdf_estatuto`)
)
SELECT 
rut
from ruts
order by random
LIMIT 100

);