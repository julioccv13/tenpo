import argparse
import pandas as pd
import json
from core.ports import Process
from core.google import storage, bigquery
from google.cloud import pubsub







QUERY_PATH_SII = "gs://{bucket_name}/034/queries/revision_sii.sql"
QUERY_PATH_E1D = "gs://{bucket_name}/034/queries/revision_e1d.sql"
TOPIC_SII = "sii_consulta_situacion_tributaria"
TOPIC_E1D = "e1d_rut_estatuto"


class Process034(Process):
    
    def __init__(self, **kwargs):
        self._bucket_name = kwargs["bucket_name"]
        self._ds_nodash = kwargs["ds_nodash"]
        self._project_id = kwargs["project_id"]
        self._project_source_1 = kwargs["project_source_1"]
        self._project_source_2 = kwargs["project_source_2"]
        self._project_source_3 = kwargs["project_source_3"]
        self._project_target = kwargs["project_target"]
    
    def run(self):
        self._query_sii()
        self._publish_table_rows(topic=TOPIC_SII, source='sii')
        self._query_e1d()
        self._publish_table_rows(topic=TOPIC_E1D, source='e1d')
        
        
    
    def _query_sii(self):
        self._execute_query(path=QUERY_PATH_SII)
        
    def _query_e1d(self):
        self._execute_query(path=QUERY_PATH_E1D)

    
    def _execute_query(self, path=None, query=None):
        if query is None:
            query_path = path.format(**{"bucket_name": self._bucket_name})
            query = storage.get_blob_as_string(query_path)
        query = self._parse_query(query)
        bigquery.execute_query(query)
    
    def _publish_table_rows(self, topic, source):
        bq_project_id = self._project_target

        publisher = pubsub.PublisherClient()
        topic_name = 'projects/{project_id}/topics/{topic}'.format(
            project_id=self._project_id,
            topic=topic,  
        )
        table_or_query = f"select * from {self._project_target}.tmp.{source}"
        data = pd.read_gbq(table_or_query, project_id=bq_project_id)
        messages = data.to_dict(orient='records')
        
        futures = [publisher.publish(topic_name, json.dumps(message).encode('utf-8')) for message in messages]

    def _parse_query(self, query):
        return (query
                .replace(r"{{ds_nodash}}", self._ds_nodash)
                .replace(r"${project_source_1}", self._project_source_1)
                .replace(r"${project_source_2}", self._project_source_2)
                .replace(r"${project_source_3}", self._project_source_3)
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
    parser.add_argument("-PS3",
                        "--project_source_3",
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
    process = Process034(**args)
    process.run()