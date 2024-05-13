import argparse
from core.ports import Process
from core.google import storage, bigquery







PARAMETERS_PATH = "gs://{bucket_name}/041/queries/01_parameters.sql"
QUERY_PATH = "gs://{bucket_name}/041/queries/02_query.sql"
INSERT_PATH = "gs://{bucket_name}/041/queries/03_insert.sql"



class Process041(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_target = kwargs["project_target"]
        self._period = kwargs["period"]
    
    def run(self):
        self._query_parameters()
        self._query()
        self._insert()
        
        
    
    def _query_parameters(self):
        self._execute_query(path=PARAMETERS_PATH)
        
    def _query(self):
        self._execute_query(path=QUERY_PATH)
        
    def _insert(self):
        self._execute_query(path=INSERT_PATH)

    
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
                .replace(r"${project_target}", self._project_target)
                .replace(r"{{period}}", self._period)
                .format(**{"project_id": self._project_id})
                )

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-D",
                        "--ds_nodash",
                        type=str,
                        required=True)
    parser.add_argument("-BN",
                        "--bucket-name",
                        type=str,
                        required=True)
    parser.add_argument("-PI",
                        "--project_id",
                        type=str,
                        required=True)
    parser.add_argument("-PS",
                        "--project_source",
                        type=str,
                        required=True)
    parser.add_argument("-PT",
                        "--project_target",
                        type=str,
                        required=True)
    parser.add_argument("-P",
                    "--period",
                    type=str,
                    required=True)
    

    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = get_args()
    process = Process041(**args)
    process.run()