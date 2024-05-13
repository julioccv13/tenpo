from core.google import bigquery, storage
from core.ports import Process
import logging
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
current_ds = datetime.date.fromordinal(datetime.date.today().toordinal()-1).strftime('%Y%m%d')

class Process119_Calculte_Targets(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = current_ds
        self._project_target = kwargs["project_target"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._raw_query = kwargs["raw_query"]

    def run(self):
        self._run_queries(self._raw_query)

    def _run_queries(self,raw_query):
        query = self._parse_query(raw_query)
        bigquery.execute_query(query)

    def _parse_query(self, query) -> str:
        """This function will replace and inject any query parameters in the sql file in order to execute the sql script"""
        return (
                query.replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"${project_source_1}", self._project_source_1)
                .replace(r"${project_source_2}", self._project_source_2)
                .replace(r"${project_target}", self._project_target)
            )