from core.ports import Process
import requests
import logging
from google.cloud import storage

class AutomaticCashbackUat(Process):
    def __init__(self, **kwargs):
        self._bucket_path = kwargs["bucket_path"]
        self._campaing_url = kwargs["campaing_url"]
        self._segment_name = kwargs["segment_name"]
        self._env = kwargs["environment"]
        self._campaign_id= None
        self._headers = {'Content-Type': 'application/json'}
    
    def run(self):
        self._campaign_id = self._get_campaing_id()
        self._send_petition()
       
    def _get_campaing_id(self) -> str:
        client = storage.Client()
        bucket = client.get_bucket(f'{self._env}_campaign_streaming_files')
        blob = bucket.get_blob('segments_sql_files/uat/campaign_id.txt')
        campaing_id = (blob.download_as_string()).decode('utf-8')
        return campaing_id

    def _send_petition(self):
        request = requests.post(self._campaing_url, data=self._get_body(), headers=self._headers)
        if request.status_code == 200:
            logging.info(f'Response text: {request.text} and Status Code: {request.status_code}')
        elif request.status_code == 400 and 'No se ha obtuvo resultados en el segmento' in request.text:
            logging.info(f'Response text: {request.text}, Response Reason: {request.reason} and Status Code: {request.status_code}')
        elif request.status_code == 404 and 'No se ha obtuvo resultados en el segmento' in request.text:
            logging.info(f'Response text: {request.text}, Response Reason: {request.reason} and Status Code: {request.status_code}')
        elif request.status_code >= 499:
            raise Exception(f"Error de comunicacion con el servicio de campañas: {request.text}")
        else:
            raise Exception(f'An error ocurred: {request.text} with status code: {request.status_code}')
    
    def _get_body(self) -> str:
        str_body = '(\"campaign_id\":\"{campaign_id}\",\"segment_sql_file_path\":\"gs://{bucket_path}/{segment_name}\")'.format(campaign_id=self._campaign_id,bucket_path=self._bucket_path,segment_name=self._segment_name)
        body = str_body.replace("(","{").replace(")","}")
        return body