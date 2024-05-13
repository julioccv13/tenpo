import argparse
from core.ports import Process
from core.google import storage, bigquery







PARAMETERS_PATH = "gs://{bucket_name}/012/queries/01_parameters.sql"
INSERT_PATH = "gs://{bucket_name}/012/queries/02_insert.sql"



class Process012(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source = kwargs["project_source"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._query_parameters()
        self._query_insert()
        
        
    
    def _query_parameters(self):
        self._execute_query(path=PARAMETERS_PATH)
        
    def _query_insert(self):
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
                .replace(r"${project_source}", self._project_source)
                .replace(r"${project_target}", self._project_target)
                .format(**{"project_id": self._project_id})
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
    parser.add_argument("-PS",
                        "--project_source",
                        type=str,
                        required=True)
    parser.add_argument("-PT",
                        "--project_target",
                        type=str,
                        required=True)
    

    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = get_args()
    process = Process012(**args)
    process.run()