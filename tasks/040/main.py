# #%%
from string import Template
from core.google import storage, bigquery
from core.ports import Process
import argparse


CALCULATE_QUERY_PATH = "gs://{bucket_name}/040_Cliente_3M/queries/cliente_3m.sql"


class Process040_3M(Process):

    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source = kwargs["project_source"]
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
        template = template.substitute({"project_source": self._project_source,
                                        "project_target": self._project_target})
        return (template
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .format(**{"project_id": self._project_id})
                )


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D",
                        "--ds-nodash",
                        type=str,
                        required=True)
    parser.add_argument("-BN",
                        "--bucket-name",
                        type=str,
                        required=True)
    parser.add_argument("-PI",
                        "--project-id",
                        type=str,
                        required=True)
    parser.add_argument("-PS",
                        "--project-source",
                        type=str,
                        required=True)
    parser.add_argument("-PT",
                        "--project-target",
                        type=str,
                        required=True)

    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    args = get_args()
    process = Process040_3M(**args)

    process.run()
