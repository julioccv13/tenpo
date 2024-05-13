from core.google import bigquery, storage
from core.ports import Process
import logging
import datetime

CREATE_TEMPS_TABLES_PATH = "gs://{bucket_name}/112/queries"
CT_PROFILES_MAX_RECORDS_PER_PAYLOAD = 1000
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

class Process112_Generate_Sql(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = current_ds
        self._project_target = kwargs["project_target"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_source_4 = kwargs["project_source_4"]

    def run(self):
        tables_to_join = self._create_temp_tables(path=CREATE_TEMPS_TABLES_PATH)
        create_table_final = self._create_table_final_query(tables_to_join)
        self.create_table_final(create_table_final)


    def _create_temp_tables(self,path=None) -> list:
        """Funcion que lee los archivos del bucket y guarda el nombre de las queries"""
        tables_to_join = []
        queries_path = path.format(**{"bucket_name": self._bucket_name})
        list_of_queries = storage.get_blobs_list_prefix_path_as_strings(queries_path)
        
        for query_path in list_of_queries:
            if len(query_path[1]) > 1:
                position = query_path[1].find('DROP')
                sql_statement = query_path[1][position:].split(";")
                table = sql_statement[0].replace('DROP TABLE IF EXISTS','')
                tables_to_join.append(table)
        
        return tables_to_join
    
    def _create_table_final_query(self,tables_to_join) -> str:
        """A partir de los archivos leidos, accede a los nombres de las queries guardadas y crea la query de inserccion final por medio de un for loop """
        QUERY = []
        CREATE_QUERY_FINAL_TABLE = str()
        query_inicial = f"""
            DROP TABLE IF EXISTS `{self._project_target}.tmp.table_final_{current_ds}`;
            CREATE TABLE `{self._project_target}.tmp.table_final_{current_ds}` AS (
            with table_final as (
            select * from
        """
        query_final = """
            )
            SELECT
            identity
            ,to_json_string((
                SELECT AS STRUCT * EXCEPT(identity)
                FROM UNNEST([t])
            )) as profileData,
            row_number() over () as row,
            0 as bloque
            from table_final t
            );
        """
        for index, table in enumerate(tables_to_join):
            if index == 0:
                QUERY.append(query_inicial)
                QUERY.append(table)
                QUERY.append('\n')
            elif index == len(tables_to_join)-1:
                QUERY.append('full outer join')
                QUERY.append(table)
                QUERY.append('USING (identity)')
                QUERY.append(query_final)
            else:
                QUERY.append('full outer join')
                QUERY.append(table)
                QUERY.append('USING (identity)')
                QUERY.append('\n')
        
        CREATE_QUERY_FINAL_TABLE = ' '.join(QUERY)
        return CREATE_QUERY_FINAL_TABLE
    
    def _parse_query(self, query) -> str:
        """This function will replace and inject any query parameters in the sql file in order to execute the sql script"""
        return (
            query.replace(r"{{ds_nodash}}", self._ds_nodash)
            .replace(r"${project_source_1}", self._project_source_1)
            .replace(r"${project_source_2}", self._project_source_2)
            .replace(r"${project_source_3}", self._project_source_3)
            .replace(r"${project_source_4}", self._project_source_4)
            .replace(r"${project_target}", self._project_target)
        )

    def create_table_final(self,query_final):
        query = self._parse_query(query_final)
        bigquery.execute_query(query)