import argparse
from core.ports import Process
from core.google import storage, bigquery
import pandas as pd


PARAMETERS_PATH = "gs://{bucket_name}/104/01_Parametros.sql"
QUERY_PATH_1 = "gs://{bucket_name}/104/02_Descriptivos.sql"
QUERY_PATH_2 = "gs://{bucket_name}/104/03_Puntuacion.sql"
INSERT_PATH = "gs://{bucket_name}/104/04_Insert.sql"


class Process104(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._generate_parameters()
        self._generate_query_1()
        self._generate_query_2()
        self._generate_insert()
             
    def _generate_parameters(self):
        self._execute_query(path=PARAMETERS_PATH)
          
    def _generate_query_1(self):
        self._execute_query(path=QUERY_PATH_1)

    def _generate_query_2(self):
        self._execute_query(path=QUERY_PATH_2)
    
    def _generate_insert(self):
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
    parser.add_argument("-PS1",
                        "--project_source_1",
                        type=str,
                        required=True)
    parser.add_argument("-PS2",
                        "--project_source_2",
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
    process = Process104(**args)
    process.run()