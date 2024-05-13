import ast

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_unixtime, col, lit


from core.ports import Process
from core.spark import Spark
from core.google import bigquery

GCS_SOURCE_TEMPLATE = "gs://{bucket_name}/{folder_name}/{ds_nodash}/*.json"
TABLES = {
    "stc_registro_consultas": {
        "create_ddl": """
        CREATE TABLE IF NOT EXISTS `{project_id}.sii.stc_registro_consultas` (
            rut STRING,
            autorizado_impuestos_monex INTEGER,
            fecha_consulta DATE,
            fecha_inicio_actividades DATE,
            inicia_actividades INTEGER,
            menor_tamanio_propyme INTEGER,
            nombre_razon_social STRING
        );
        """,
        "query": """
        DELETE FROM `{project_id}.sii.stc_registro_consultas` where DATE(fecha_consulta) = '{ds}';
        INSERT INTO `{project_id}.sii.stc_registro_consultas`
        (rut,
        autorizado_impuestos_monex,
        fecha_consulta,
        fecha_inicio_actividades,
        inicia_actividades,
        menor_tamanio_propyme,
        nombre_razon_social
        )
            SELECT DISTINCT
                CAST(rut AS STRING),
                CAST(autorizado_impuestos_monex AS INTEGER),
                DATE(fecha_consulta),
                DATE(fecha_inicio_actividades), 
                CAST(inicia_actividades AS INTEGER),
                CAST(menor_tamanio_propyme AS INTEGER),
                CAST(nombre_razon_social AS STRING),
            FROM `{project_id}.temp.stc_registro_consultas_{ds_nodash}`;
         """,
        "temp_table": "{project_id}:temp.stc_registro_consultas_{ds_nodash}",
    },
    "stc_actividades": {
        "create_ddl": """
        CREATE TABLE IF NOT EXISTS `{project_id}.sii.stc_actividades` (
            rut STRING,
            actividad STRING,
            afecta_iva INTEGER,
            categoria STRING,
            codigo_actividad INTEGER,
            fecha_consulta DATE,
            fecha_inicio_actividad DATE
        );
        """,
        "query": """
        DELETE FROM `{project_id}.sii.stc_actividades` where DATE(fecha_consulta) = '{ds}';
        INSERT INTO `{project_id}.sii.stc_actividades`
        (rut,
        actividad,
        afecta_iva,
        categoria,
        codigo_actividad,
        fecha_consulta,
        fecha_inicio_actividad
        )
                SELECT DISTINCT
                CAST(rut AS STRING),
                CAST(actividad AS STRING), 
                CAST(afecta_iva AS INTEGER),
                CAST(categoria AS STRING),
                CAST(codigo_actividad AS INTEGER),
                DATE(fecha_consulta), 
                DATE(fecha_inicio_actividad), 
            FROM `{project_id}.temp.stc_actividades_{ds_nodash}`;
         """,
        "temp_table": "{project_id}:temp.stc_actividades_{ds_nodash}",
    },
    "stc_timbraje_documentos": {
        "create_ddl": """
        CREATE TABLE IF NOT EXISTS `{project_id}.sii.stc_timbraje_documentos` (
            rut STRING,
            ano_ultimo_timbraje INTEGER,
            documento STRING,
            fecha_consulta DATE
        );
        """,
        "query": """
        DELETE FROM `{project_id}.sii.stc_timbraje_documentos` where DATE(fecha_consulta) = '{ds}';
        INSERT INTO `{project_id}.sii.stc_timbraje_documentos`
        (
        rut,
        ano_ultimo_timbraje,
        documento,
        fecha_consulta
        )
                SELECT DISTINCT
                CAST(rut AS STRING),
                CAST(ano_ultimo_timbraje AS INTEGER),
                CAST(documento AS STRING), 
                DATE(fecha_consulta), 
            FROM `{project_id}.temp.stc_timbraje_documentos_{ds_nodash}`;
         """,
        "temp_table": "{project_id}:temp.stc_timbraje_documentos_{ds_nodash}",
    },
}


class Process504(Process):
    def __init__(self, *args, **kwargs):
        self.source_bucket_name = kwargs.get("source_bucket_name")
        self.temp_bucket_name = kwargs.get("temp_bucket_name")
        self.project_id = kwargs.get("project_id")
        self.ds_nodash = kwargs.get("ds_nodash")
        self.ds = kwargs.get("ds")

    def run(self):
        for table in TABLES:
            print(table)
            data = self._extract_data(table)
            data = self._transform_data(data)
            self._load_data(data, table)
            query = self._get_query_string(table)
            create_ddl = self._get_create_ddl(table)
            self._execute_query(create_ddl)  # TODO: Revisar si se deberia crear schema
            self._execute_query(query)

    def _extract_data(self, folder_name: str) -> DataFrame:
        files_path = GCS_SOURCE_TEMPLATE.format(
            bucket_name=self.source_bucket_name,
            folder_name=folder_name,
            ds_nodash=self.ds_nodash,
        )
        sc = Spark().sparkContext
        data = sc.textFile(files_path).flatMap(self._clear_json_files).toDF()
        data.cache()
        return data

    def _clear_json_files(self, json_data):
        return ast.literal_eval(json_data.replace("null", "None"))

    def _transform_data(self, data: DataFrame) -> DataFrame:
        columns = data.columns
        variable = None
        if "fecha_inicio_actividades" in columns:
            variable = "fecha_inicio_actividades"
        elif "fecha_inicio_actividad" in columns:
            variable = "fecha_inicio_actividad"

        if variable:
            # Convert ms datetime to datetime
            data = data.withColumn(variable, from_unixtime(col(variable) / lit(1000.0)))
        data = data.withColumn(
            "fecha_consulta", from_unixtime(col("fecha_consulta") / lit(1000.0))
        )
        return data.repartition(100)

    def _load_data(self, data: DataFrame, table_name: str) -> None:
        table = self._get_temp_table(table_name)
        data.write.format("bigquery").option("table", table).option(
            "temporaryGcsBucket", self.temp_bucket_name
        ).mode("overwrite").save()

    def _get_temp_table(self, table_name: str) -> str:
        return self._get_table_info(table_name, "temp_table")

    def _get_create_ddl(self, table_name: str) -> str:
        return self._get_table_info(table_name, "create_ddl")

    def _get_query_string(self, table_name: str) -> str:
        return self._get_table_info(table_name, "query")

    def _get_table_info(self, table_name: str, info: str) -> str:
        return TABLES[table_name][info].format(
            project_id=self.project_id, ds_nodash=self.ds_nodash, ds=self.ds
        )

    def _execute_query(self, query=None) -> None:
        bigquery.execute_query(query)
