import argparse
from core.ports import Process
from core.google import storage, bigquery


QUERY_PATH_1 = "gs://{bucket_name}/500/queries/01_query_tipo_mau.sql"
QUERY_PATH_2 = "gs://{bucket_name}/500/queries/02_query_mau_detalle.sql"
QUERY_PATH_3 = "gs://{bucket_name}/500/queries/03_query_matriz.sql"
QUERY_PATH_4 = "gs://{bucket_name}/500/queries/04_query_mau_mastercard.sql"
QUERY_PATH_5 = "gs://{bucket_name}/500/queries/05_query_mau_pdc.sql"


class Process500(Process):

    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source = kwargs["project_source"]
        self._project_target = kwargs["project_target"]

    def run(self):
        self._generate_query_1()
        self._generate_query_2()
        self._generate_query_3()
        self._generate_query_4()
        self._generate_query_5()

    def _generate_query_1(self):
        self._execute_query(path=QUERY_PATH_1)

    def _generate_query_2(self):
        self._execute_query(path=QUERY_PATH_2)

    def _generate_query_3(self):
        self._execute_query(path=QUERY_PATH_3)

    def _generate_query_4(self):
        self._execute_query(path=QUERY_PATH_4)

    def _generate_query_5(self):
        self._execute_query(path=QUERY_PATH_5)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"${project_source}", self._project_source)
                .replace(r"${project_target}", self._project_target)
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
    process = Process500(**args)
    process.run()
