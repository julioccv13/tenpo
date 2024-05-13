import argparse
from core.ports import Process
from core.google import storage, bigquery


PARAMETERS_QUERY_PATH = "gs://{bucket_name}/037/01_Parametros.sql"
CALCULATE_QUERY_PATH = "gs://{bucket_name}/037/02_Query.sql"
INSERT_QUERY_PATH = "gs://{bucket_name}/037/03_Insert.sql"

class Process037(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._period= kwargs["period"] 
        self._project_source_1=kwargs["project_source_1"]
        self._project_source_2=kwargs["project_source_2"]
        self._project_source_3=kwargs["project_source_3"]
    
    def run(self):
        self._generate_parameters()
        self._calculate()
        self._insert()
    
    def _generate_parameters(self):
        self._execute_query(path=PARAMETERS_QUERY_PATH)
    
    def _calculate(self):
        self._execute_query(path=CALCULATE_QUERY_PATH)
    
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
                .replace(r"{{project_id}}", self._project_id)
                .replace(r"{{period}}", self._period)
                .replace(r"{{project_source_1}}", self._project_source_1)
                .replace(r"{{project_source_2}}", self._project_source_2)
                .replace(r"{{project_source_3}}", self._project_source_3)
                )

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D",
                        "--ds_nodash",
                        type=str,
                        required=True)
    parser.add_argument("-BN",
                        "--bucket_name",
                        type=str,
                        required=True)
    parser.add_argument("-PI",
                        "--project_id",
                        type=str,
                        required=True)
    parser.add_argument("-PE",
                        "--period",
                        type=str,
                        required=True)
    parser.add_argument("-PS1",
                        "--project_source_1",
                        type=str,
                        required=True)
    parser.add_argument("-PS2",
                        "--project_source_2",
                        type=str,
                        required=True)
    parser.add_argument("-PS3",
                        "--project_source_3",
                        type=str,
                        required=True)

    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = get_args()
    process = Process037(**args)

    process.run()