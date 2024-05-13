from core.google import bigquery, storage
from core.ports import Process
import datetime
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

CREATE_TEMPS_TABLES_PATH = "gs://{bucket_name}/112/queries"
class Process112_Calculate_Queries(Process):
    def __init__(self, **kwargs):
        self._env = kwargs["env"]
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_source_4 = kwargs["project_source_4"]
        self._custom_number = kwargs["custom_number"]
        self._range_interval = kwargs["range_interval"]
        self._begin_range_interval = self._range_interval[0]
        self._end_range_interval = self._range_interval[-1]
    
    def run(self):
        self._create_temp_tables()

    def _create_temp_tables(self,path=CREATE_TEMPS_TABLES_PATH) -> list:
        queries_path = path.format(**{"bucket_name": f"{self._env}-source-files"})
        list_of_queries = storage.get_blobs_list_prefix_path_as_strings(queries_path)
        for index, query_path in enumerate(list_of_queries):
            if  index >= self._begin_range_interval and index <= self._end_range_interval:
                if query_path[1] != '':
                    query = self._parse_query(query_path[1])
                    bigquery.execute_query(query)

    def _parse_query(self,query) -> str:
        """This function will replace and inject any query parameters in the sql file in order to execute the sql script"""
        return (
            query.replace(r"{{ds_nodash}}",current_ds)
            .replace(r"${project_target}", self._project_target)
            .replace(r"${project_source_1}", self._project_source_1)
            .replace(r"${project_source_2}", self._project_source_2)
            .replace(r"${project_source_3}", self._project_source_3)
            .replace(r"${project_source_4}", self._project_source_4)
    )