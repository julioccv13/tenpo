import argparse
import pandas as pd
from datetime import date
from core.ports import Process
from core.google import storage

class Solicitud_Nueva_Direccion(Process):
    
    def __init__(self, **kwargs):
        self._gcs_project = kwargs["gcs_project"]
        self._gcs_folder = kwargs["gcs_folder"]
        self._bq_project = kwargs["bq_project"]
        self._bq_table = kwargs["bq_table"]
        self._bucket_name=''
        self._blob_source_name=''
        self._bucket_name,self._blob_source_name=storage.get_bucket_and_blob_from_path(path=self._gcs_folder)
        self._path_from='gs://{}/{}'.format(self._bucket_name,self._blob_source_name)
        self._path_to='gs://{}/{}'.format(self._bucket_name,self._blob_source_name).replace('temp','history')
    
    def run(self):
        self._process()
        self._move_files_bucket()
    
    def _process(self):
        today = date.today()
        blobs=storage.get_blobs_list_prefix_path(path=self._gcs_folder)
        for blob in blobs:
            blob_path='gs://'+self._bucket_name+'/'+blob.name
            if ".csv"  in blob_path: 
                df = pd.read_csv(blob_path)
                df['date'] = today.strftime("%Y-%m-%d")
                df.to_gbq(destination_table=self._bq_table, project_id=self._bq_project, if_exists='append')
    
    def _move_files_bucket(self):
        storage.move_files(path_from=self._path_from,path_to=self._path_to)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-GSP",
                        "--gcs_project",
                        type=str,
                        required=True)
    parser.add_argument("-BLP",
                        "--gcs_folder",
                        type=str,
                        required=True)
    parser.add_argument("-BQP",
                        "--bq_project",
                        type=str,
                        required=True)
    parser.add_argument("-BQT",
                        "--bq_table",
                        type=str,
                        required=True)
    args = parser.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = get_args()
    process = Solicitud_Nueva_Direccion(**args)

    process.run()