SELECT DISTINCT * FROM `${project_name}.${dataset}.C78_caratula` WHERE EXECUTION_DATE = (SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.C78_caratula`
)