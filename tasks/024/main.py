import argparse
from core.ports import Process
from core.google import storage, bigquery


QUERY_PATH = "gs://{bucket_name}/024/queries/query.sql"


class Process024(Process):

    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_source_4 = kwargs["project_source_4"]
        self._project_target = kwargs["project_target"]

    def run(self):
        self._generate_query()

    def _generate_query(self):
        self._execute_query(path=QUERY_PATH)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"${project_source_1}", self._project_source_1)
                .replace(r"${project_source_2}", self._project_source_2)
                .replace(r"${project_source_3}", self._project_source_3)
                .replace(r"${project_source_4}", self._project_source_4)
                .replace(r"${project_target}", self._project_target)
                .format(**{"project_id": self._project_id})
                )


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-P",
                        "--period",
                        type=str,
                        enum=["daily", "hourly", "weekly"],
                        required=True)
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
    parser.add_argument("-PS2",
                        "--project-source-2",
                        type=str,
                        required=True)
    parser.add_argument("-PS3",
                        "--project-source-3",
                        type=str,
                        required=True)
    parser.add_argument("-PS4",
                        "--project-source-4",
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
    process = Process024(**args)
    # run
    process.run()
