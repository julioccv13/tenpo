import argparse
from core.ports import Process
from core.google import storage, bigquery


PERIOD_QUERIES = {
    "daily": "gs://{bucket_name}/038/queries/periods/01_daily.sql",
    "weekly": "gs://{bucket_name}/038/queries/periods/01_weekly.sql",
    "monthly": "gs://{bucket_name}/038/queries/periods/01_monthly.sql",
}
QUERY_PATH = "gs://{bucket_name}/038/queries/02_Query.sql"
INSERT_QUERY_PATH = "gs://{bucket_name}/038/queries/03_Insert.sql"


class Process038(Process):

    def __init__(self, **kwargs):
        self._period = kwargs["period"]
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source = kwargs["project_source"]
        self._project_target = kwargs["project_target"]

    def run(self):
        self._generate_period_table()
        self._generate_query()
        self._insert()

    def _generate_query(self):
        self._execute_query(path=QUERY_PATH)

    def _generate_period_table(self):
        self._execute_query(path=PERIOD_QUERIES[self._period])

    def _insert(self):
        self._execute_query(path=INSERT_QUERY_PATH)

    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"{{period}}", self._period)
                .replace(r"${project_source}", self._project_source)
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
    process = Process038(**args)
    process.run()
