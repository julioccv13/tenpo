from string import Template

from core.google import bigquery, storage
from core.ports import Process

CALCULATE_QUERY_PATH = "gs://{bucket_name}/045/publico.sql"


class Process045(Process):
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._ds = kwargs["ds"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_target = kwargs["project_target"]

    def run(self):
        self._calculate()

    def _calculate(self):
        self._execute_query(path=CALCULATE_QUERY_PATH)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        template = Template(query)
        template = template.substitute(
            {
                "project_source_1": self._project_source_1,
                "project_source_2": self._project_source_2,
                "project_source_3": self._project_source_3,
                "project_target": self._project_target,
            }
        )
        return (
            template.replace(r"{{ds_nodash}}", self._ds_nodash)
            .replace(r"{{ds}}", self._ds)
            .format(**{"project_id": self._project_id})
        )
