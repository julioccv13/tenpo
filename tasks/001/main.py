import argparse
from core.ports import Process
from core.google import storage, bigquery
import pandas as pd
from datetime import datetime




INSERT_PATH = "gs://{bucket_name}/001/queries/01_insert.sql"



class Process001(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._sbif_api_key = kwargs["sbif_api_key"]
        self._project_source = kwargs["project_source"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._get_dolar()
        self._insert_query()
        
    
    def _insert_query(self):
        self._execute_query(path=INSERT_PATH)

    
    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)
    
    def _get_dolar(self):

        OUTPUT_PROJECT_ID = self._project_target
        OUTPUT_TABLE = f"temp.DE001_{self._ds_nodash}_Dolares"
        SBIF_API_KEY = self._sbif_api_key
        


        
        year = str(datetime.strptime(self._ds_nodash, '%Y%m%d').year)
        
        
        url = f"https://api.sbif.cl/api-sbifv3/recursos_api/dolar/{year}?apikey={SBIF_API_KEY}&formato=json"

        data = pd.read_json(url)['Dolares'].apply(pd.Series)
        
        data['Valor'] = data['Valor'].str.replace(',', '.').astype(float)


        data.to_gbq(OUTPUT_TABLE, project_id=OUTPUT_PROJECT_ID, if_exists='replace', table_schema=[{'name': 'Fecha', 'type': 'DATE'}])

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
    parser.add_argument("-SB",
                        "--sbif_api_key",
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
    process = Process001(**args)
    process.run()