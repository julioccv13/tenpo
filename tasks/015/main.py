import argparse
from core.ports import Process
from core.google import storage, bigquery


PARAMS_02_QUERY_PATH = "gs://{bucket_name}/015/queries/02_Params_Calculo.sql"
PARAMS_03_QUERY_PATH = "gs://{bucket_name}/015/queries/03_Params_Calculo.sql"


class Process015(Process):

    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]  # tenpo-bi-prod

    def run(self):
        self._execute_query(PARAMS_02_QUERY_PATH)
        self._execute_query(PARAMS_03_QUERY_PATH)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .format(**{
                    "project_id": self._project_id,
                    "project_source_1": self._project_source_1,
                })
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
    parser.add_argument("-PS1",
                        "--project-source-1",
                        type=str,
                        required=True)

    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    args = get_args()
    process = Process015(**args)
    process.run()
