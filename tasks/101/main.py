import argparse
import pandas as pd
from core.google import bigquery
from core.google.storage import get_blob_as_string
from google.cloud import storage

storage_client = storage.Client()

class Process101:
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._bucket_name_input = kwargs["bucket_name_input"]
        self._bucket_name_output = kwargs["bucket_name_output"]
        self._filename = kwargs["filename"]
        self._output = kwargs["output"]
        self._ds = kwargs["ds"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_target = kwargs["project_target"]
        self.project3 = kwargs["project3"]
        self.project2 = kwargs["project2"]
        self._env = kwargs["env"]

    def run(self):
        self._process_selyt_data_fn()
        self._join_data()
        self._send_data()
        self._insert_data()
        

    def _process_selyt_data_fn(self):
        def fix_rut(rut_string):
            return rut_string[:-1] + "-" + rut_string[-1]

        bucket = storage_client.bucket(self._bucket_name_input)
        blob = bucket.blob(self._filename)
        data_bytes = blob.download_as_bytes()
        df = pd.read_excel(data_bytes)
        df["rut_cliente"] = df["rut_cliente"].astype(str).apply(fix_rut)
        del df["fecha_registro_Tenpo"]
        del df["fecha_primera_compra"]
        table_name = f"temp.selyt_input_{self._ds}"
        df.to_gbq(table_name, project_id=self._project_target, if_exists="replace")

    def _join_data(self):
        query_path = f"gs://{self._bucket_name}/101/join_query.sql"
        query = get_blob_as_string(query_path)
        query = (
            query.replace("${project_target}", self._project_target)
            .replace("${project3}", self.project3)
            .replace("${project2}", self.project2)
            .replace("${ds_nodash}", self._ds_nodash)
            .replace("${ds}", self._ds)
        )
        
        bigquery.execute_query(query)  

    def _send_data(self):
        bucket = storage_client.get_bucket(self._bucket_name_output)
        query = f"SELECT * FROM `tenpo-datalake-{self._env}.temp.selyt_out_{self._ds_nodash}`"
        joined_data = pd.read_gbq(query, project_id=self._project_target)
        bucket.blob("seguimiento_tenpo_selyt.xlsx").upload_from_string(
            joined_data.to_csv(), "seguimiento_tenpo_selyt/xlsx"
        )
    
    def _insert_data(self):
        query_path = f"gs://{self._bucket_name}/101/insert.sql"
        query = get_blob_as_string(query_path)
        query = (
            query.replace("${project_target}", self._project_target)
            .replace("${ds_nodash}", self._ds_nodash)
            )
        bigquery.execute_query(query)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-DS", "--ds", type=str, required=True)
    parser.add_argument("-DSN", "--ds_nodash", type=str, required=True)
    parser.add_argument("-BN", "--bucket-name", type=str, required=True)
    parser.add_argument("-BNI", "--bucket_name_input", type=str, required=True)
    parser.add_argument("-BNO", "--bucket_name_output", type=str, required=True)
    parser.add_argument("-FI", "--filename", type=str, required=True)
    parser.add_argument("-O", "--output", type=str, required=True)
    parser.add_argument("-PI", "--project_id", type=str, required=True)
    parser.add_argument("-PT", "--project_target", type=str, required=True)
    parser.add_argument("-PS1", "--project3", type=str, required=True)
    parser.add_argument("-PS2", "--project2", type=str, required=True)
    parser.add_argument("-ENV", "--env", type=str, required=True)

    args = parser.parse_args()
    return vars(args)


if __name__ == "__main__":
    args = get_args()
    process = Process101(**args)
    process.run()
