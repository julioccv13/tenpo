WITH segmento AS (
select distinct (e.user) user
from (
select distinct eco.user
 from  `{project_source_1}.economics.economics`  as eco
where eco.fecha >= date_add(CURRENT_DATE(),INTERVAL -6 MONTH)
and (eco.rubro_recod = 'Pago de Cuentas' or eco.comercio_recod like '%autopistas%')
and eco.monto >= 1
and (eco.nombre in ('Televisión','Gas','Financiera ','Cementerio ','Luz ',' Banda Ancha Móvil ','Agua ','Telecomunicaciones','Telefonía Fija','Telefonía Móvil') or eco.comercio_recod like '%autopistas%')
union all
select distinct (user) user
from  `{project_source_1}.economics.economics`
where linea like '%master%'
and monto > 1
and fecha >= date_add(CURRENT_DATE(),INTERVAL -3 MONTH)
and user not in (select distinct eco.user
 from  `{project_source_1}.economics.economics`  as eco
where eco.fecha >= date_add(CURRENT_DATE(),INTERVAL -24 MONTH)
and (eco.rubro_recod = 'Pago de Cuentas' or eco.comercio_recod like '%autopistas%')
and eco.monto >= 1
and (eco.nombre in ('Televisión','Gas','Financiera ','Cementerio ','Luz ',' Banda Ancha Móvil ','Agua ','Telecomunicaciones','Telefonía Fija','Telefonía Móvil') or eco.comercio_recod like '%autopistas%')
) ) e
)

SELECT
    'i' AS type,
    user AS identity
FROM segmento