WITH PDC_HISTORICO as 
(
select user from `{project_source_1}.economics.economics` where linea = 'utility_payments' 
)
,TARJETA_MIGRAR as
(
  select user from `{project_source_1}.economics.economics` where linea = 'mastercard' and fecha >= DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY)
  and comercio_recod in 
  (
  'unired cl vespucio nor',
  'unired cl costanera no',
  'unired cl vespucio sur',
  'uniredcl r maipo',
  'vespucio norte',
  'ruta del maipo sc sa',
  'metrogas metrogas.cl',
  'unired cl autopista de',
  'unired cl cge',
  'merpago*cge',
  'unired cl mundo pacifi',
  'gas sur internet',
  'universidad san sebast',
  'univ diego portales we',
  'unired cl essbio',
  'merpago*cuentas',
  'merpago*essbio',
  'merpago*pagoschile',
  'ruta del maipo web ptt',
  'entel',
  'telefonica',
  'claro',
  'punto pagos',
  'sencillito',
  'recarga pagoya 1',
  'aguas',
  'pagos servicios limita',
  'mundo pacifico',
  'vtr',
  'pagadoor',
  'enel',
  'lipigas',
  'virgin',
  'ponp ltd',
  'movistar',
  'directv',
  'venta de bolsas',
  'wom',
  'chilquinta',
  'fiverreu',
  'esval',
  'gasco',
  'quefacil cl',
  'abastible',
  'gtd',
  'essal',
  'duoc',
  'nueva atacama',
  'amawa',
  'gce',
  'cementerio',
  'telsur'
  )  
)

select 'i' as type, user as identity from TARJETA_MIGRAR where user not in (select user from PDC_HISTORICO)